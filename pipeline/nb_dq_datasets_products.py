#!/usr/bin/env python
# coding: utf-8

# <div align="right">
#   <img src="Resources/logo_unir.png" alt="Logo UNIR" height="150px" width="25%">
# </div>
# 
# # **Universidad Internacional de La Rioja**
# ## Escuela Superior de Ingeniería y Tecnología
# ### Máster Universitario en Análisis y Visualización de Datos Masivos / Visual Analytics and Big Data
# 
# ### **Trabajo Final de Máster**
# #### Presentado por:
# - Cepeda Ramos, Jefferson
# - Mosquera Arce, Samek Fernando 

# ---
# ## **Objetivo del notebook: Proceso de calidad datasets Productos**
# 
# Este proceso tiene como objetivo realizar un proceso de calidad y transformación de datasets de productos almacenados en la **capa Bronze** de un data lake en GCP (`gs://lk_bronze/GSC/products/`) para prepararlos y moverlos a la **capa Silver** (`gs://lk_silver/GSC/products/`). Los datos provienen de múltiples categorías (`software`, `musical_instruments`, `video_games`) y están en formato **Parquet**.
# 
# El flujo general del proceso contempla los siguientes pasos:
# 
# 1.  **Unificación de Datos**:
#     * Leer los datasets de productos de las categorías `software`, `musical_instruments`, y `video_games` desde la capa Bronze.
#     * Unificar todos los DataFrames en un único DataFrame de Spark.
# 
# 2.  **Eliminación de Columna Vacía**:
#     * Eliminar la columna `bought_together`, ya que se ha identificado que está completamente vacía en todos los datasets.
# 
# 3.  **Manejo de Valores Duplicados**:
#     * Eliminar registros duplicados dentro del DataFrame unificado, considerando todas las columnas para una deduplicación completa.
# 
# 4.  **Manejo de Valores Nulos y Limpieza de Atributos Críticos**:
#     * **`price`**: Convertir el campo `price` a tipo `FloatType()`. Eliminar los registros donde el `price` sea nulo o no pueda ser casteado a un valor numérico válido, debido a su criticidad y la dificultad de una imputación precisa.
#     * **`main_category`**: Eliminar registros donde `main_category` sea nulo, ya que es un atributo fundamental para la clasificación del producto.
#     * **`store`**: Eliminar registros donde `store` sea nulo, por su importancia en la identificación de la procedencia del producto.
#     * **`rating_number` y `average_rating`**: Mantener los registros incluso si estos campos son nulos, ya que un producto sin calificaciones es un escenario válido y no requiere imputación para este nivel de calidad.
# 
# 5.  **Manejo de Campos Anidados (`images`, `videos`, `details`)**:
#     * Mantener la estructura anidada de los campos `images` (ArrayType de StructType), `videos` (ArrayType de StructType), y `details` (StructType) dentro del DataFrame principal. Esta práctica preserva la integridad del producto y es adecuada para la capa Silver, permitiendo mayor flexibilidad para explotar estos campos en capas posteriores (Gold) si es necesario.
# 
# 6.  **Almacenamiento en Capa Silver**:
#     * Los DataFrames resultantes del proceso de calidad se guardarán en la capa Silver, en formato **Parquet**.
#     * El almacenamiento se realizará por categoría, en las rutas `gs://lk_silver/GSC/products/ + [categoria]`, utilizando el modo `overwrite` para garantizar que los datos más recientes reemplacen a los anteriores.
# 
# ---

# ## Instanciar SparkSession

# In[1]:


# Inicializar SparkSession si no está ya disponible
try:
    spark
except NameError:
    spark = SparkSession.builder.appName("AmazonReviewsProcessing").getOrCreate()

print("SparkSession inicializada.")


# ## Insertar librerías

# In[2]:


# Importar las librerías necesarias
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    LongType,
    ArrayType,
)
from pyspark.sql.functions import col, lit, when, regexp_replace, explode_outer


# ## Definir Parámetros Base

# In[3]:


# Definir el esquema explícito para los datasets de metadatos (productos)
# Ajustes:
# - 'price' a StringType() inicialmente para manejar valores no numéricos antes de castear.
# - 'rating_number' a LongType() para mayor rango que IntType.
# - Los campos de 'details' con nombres amigables para Spark (sin espacios ni caracteres especiales).
product_schema = StructType([
    StructField("main_category", StringType(), True),
    StructField("title", StringType(), True),
    StructField("average_rating", FloatType(), True),
    StructField("rating_number", LongType(), True),
    StructField("features", ArrayType(StringType()), True),
    StructField("description", ArrayType(StringType()), True),
    StructField("price", StringType(), True),
    StructField("images", ArrayType(StructType([
        StructField("thumb", StringType(), True),
        StructField("large", StringType(), True),
        StructField("hi_res", StringType(), True),
        StructField("variant", StringType(), True)
    ])), True),
    StructField("videos", ArrayType(StructType([
        StructField("title", StringType(), True),
        StructField("url", StringType(), True),
        StructField("user_id", StringType(), True)
    ])), True),
    StructField("store", StringType(), True),
    StructField("categories", ArrayType(StringType()), True),
    StructField("details", StructType([
        StructField("release_date", StringType(), True),
        StructField("date_first_listed_on_amazon", StringType(), True),
        StructField("developed_by", StringType(), True),
        StructField("size", StringType(), True),
        StructField("version", StringType(), True),
        StructField("application_permissions", ArrayType(StringType()), True),
        StructField("minimum_operating_system", StringType(), True),
        StructField("approximate_download_time", StringType(), True)
    ]), True),
    StructField("parent_asin", StringType(), True),
    StructField("bought_together", ArrayType(StringType()), True),
    StructField("category", StringType(), True)
])

# Rutas de los datasets en GCP
bronze_paths = {
    "software": "gs://lk_bronze/GSC/products/software",
    "musical_instruments": "gs://lk_bronze/GSC/products/musical_instruments",
    "video_games": "gs://lk_bronze/GSC/products/video_games",
}

# Ruta de destino en la capa Silver
silver_path = "gs://lk_silver/GSC/products/"

print("Esquema y rutas definidas.")


# ## Leer los dataframe

# In[4]:


# Lista para almacenar los DataFrames individuales
dfs = []

# Leer cada dataset parquet desde "gs://lk_bronze/GSC/"
for category, path in bronze_paths.items():
    print(f"Leyendo datos de: {path} con categoría: {category}")
    df = spark.read.schema(product_schema).parquet(path)
    dfs.append(df)

# Unificar todos los DataFrames
print("Unificando DataFrames...")
df_products = dfs[0]
for i in range(1, len(dfs)):
    df_products = df_products.unionByName(dfs[i], allowMissingColumns=True)

print(f"Total de registros unificados antes de la calidad: {df_products.count()}")


# ## **Proceso de calidad de datos**

# In[5]:


print("Iniciando proceso de calidad de datos...")

# Eliminar la columna 'bought_together'
if "bought_together" in df_products.columns:
    print("Eliminando columna 'bought_together' (100% vacía).")
    df_products = df_products.drop("bought_together")
else:
    print("La columna 'bought_together' no existe o ya fue eliminada.")

# Eliminar valores duplicados
print("Eliminando valores duplicados considerando todos los atributos.")
initial_count = df_products.count()
df_products = df_products.dropDuplicates()
deduplicated_count = df_products.count()
print(f"Registros eliminados por duplicidad: {initial_count - deduplicated_count}")

# Manejo de atributos con valores NULL
# Castear 'price' a FloatType y manejar nulos
print("Procesando la columna 'price'...")
# Limpiar el campo price para asegurar que solo contenga valores numéricos válidos
# Se remueven comas si existen y se intenta convertir a float
df_products = df_products.withColumn(
    "price_cleaned",
    regexp_replace(col("price"), ",", "").cast(FloatType())
)

# Eliminar registros donde 'price_cleaned' es NULL después del casteo
# Esto manejará tanto los NULL originales como los valores que no pudieron ser casteados.
price_null_count = df_products.filter(col("price_cleaned").isNull()).count()
df_products = df_products.filter(col("price_cleaned").isNotNull())
print(f"Registros eliminados por precio nulo o inválido: {price_null_count}")

# Reemplazar la columna 'price' original con 'price_cleaned' y renombrarla
df_products = df_products.withColumn("price", col("price_cleaned")).drop("price_cleaned")

# Eliminar registros donde 'main_category' es NULL
print("Procesando la columna 'main_category'...")
main_category_null_count = df_products.filter(col("main_category").isNull()).count()
df_products = df_products.filter(col("main_category").isNotNull())
print(
    f"Registros eliminados por 'main_category' nula: {main_category_null_count}"
)

# Eliminar registros donde 'store' es NULL
print("Procesando la columna 'store'...")
store_null_count = df_products.filter(col("store").isNull()).count()
df_products = df_products.filter(col("store").isNotNull())
print(f"Registros eliminados por 'store' nula: {store_null_count}")

# Mantener registros con 'rating_number' y 'average_rating' nulos. No se hace nada aquí.
print(
    "Columnas 'rating_number' y 'average_rating': Se mantienen los valores nulos."
)

# Re-contar el total de registros después de las operaciones de calidad
final_count = df_products.count()
print(f"Total de registros después del proceso de calidad: {final_count}")

# Verificar el esquema final
df_products.printSchema()


# ## Almacenar datos limpios en capa Silver

# In[6]:


# Almacenamiento del dataset unificado en la capa Silver "gs://lk_silver/GSC/products/"
print("Guardando DataFrames procesados en la capa Silver...")

df_products.write.mode("overwrite").parquet(silver_path)

print("Proceso de calidad y almacenamiento completado.")


# ## Detener SparkSession

# In[10]:


# Detener la SparkSession
spark.stop()

