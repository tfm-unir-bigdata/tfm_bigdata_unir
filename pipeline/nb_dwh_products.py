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
# ## **Objetivo del notebook: Proceso de ingesta de datos datasets productos en la capa Gold**
# 
# Este proceso tiene como objetivo realizarla ingesta de los datos de productos de usuarios almacenados en la **capa Silver** en GCP (`gs://lk_silver/GSC/reviews/`) en la **capa Gold** en Google BigQuery, en el schema `dw_gold`.
# 
# El flujo general del proceso contempla los siguientes pasos:
# 
# 1.  **Lectura de los datos**:
#     * Leer el dataset de productos desde la capa Silver.
# 
# 2.  **Definir las tablas dimensionales**:
#     * Se definen las tablas dimensionales ingresando una llave subrrogada para los id de la relación.
# 
# 3.  **Definir la tabla de hechos**:
# 
#     * Se define una tabla de hechos con relación a las tablas dimensionales creadas.
# 
# 5.  **Almacenamiento en Capa Gold**:
#     * Se hace uso del bucket temporal **dwn_gold** para almacenar los datos y posteriomente hacer la inserción sobre la base de datos en BigQuery.
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
from pyspark.sql.functions import (
    monotonically_increasing_id, 
    col, 
    explode, 
    to_json, 
    from_json
)
from pyspark.sql.types import MapType, StringType


# ## Definir Parámetros Base

# In[3]:


# Definir el esquema explícito para los datasets de metadatos (productos)
# Rutas del datasets en GCP

# Ruta origen del dataset en la capa Silver
silver_path = "gs://lk_silver/GSC/products/"

# Nombre Schema en la capa Gold
gold_database = "proyecto-tfm-unir"
gold_schema = "dw_gold"
gold_bucket = "dwh_gold"

print("Parámetros definidos")


# ## Leer los dataframe

# In[4]:


# Leer el dataset desde la ruta en silver
print(f"Leyendo datos de: {silver_path}")
df = spark.read.parquet(silver_path)

initial_row_count = df.count()

print(f"Total de registros leidos: {initial_row_count}")

print("\nMostrando el esquema final del DataFrame:")
df.printSchema()


# ## Definición de tablas dimensionales

# In[5]:


# Es una buena práctica cachear el DataFrame si se va a usar repetidamente.
# Cachear el DataFrame para optimizar las operaciones repetidas sobre él
df.cache()

# ==============================================================================
# PARTE 1: CREACIÓN DE LAS DIMENSIONES PRINCIPALES Y SIMPLES
# ==============================================================================
print("\n--- Parte 1: Creando Dimensiones Principales y Simples ---")

# 1.1. Dimensión Principal: dim_product
# Identifica de forma única cada producto conceptual.
print("\n1.1. Creando 'dim_product'...")
dim_product = df.select("parent_asin", "title", "main_category") \
                .dropDuplicates(["parent_asin"])
dim_product = dim_product.withColumn("product_id", monotonically_increasing_id())
dim_product.printSchema()
dim_product.show(5, truncate=False)

# 1.2. Dimensión Simple: dim_store
# Contiene la lista de todas las tiendas o marcas únicas.
print("\n1.2. Creando 'dim_store'...")
dim_store = df.select(col("store").alias("store_name")) \
              .filter(col("store_name").isNotNull()) \
              .dropDuplicates()
dim_store = dim_store.withColumn("store_id", monotonically_increasing_id())
dim_store.printSchema()
dim_store.show(5, truncate=False)


# ==============================================================================
# PARTE 2: CREACIÓN DE DIMENSIONES A PARTIR DE LISTAS (Arrays)
# ==============================================================================
print("\n--- Parte 2: Creando Dimensiones a partir de Listas ---")

# 2.1. Dimensión de Categorías: dim_category
# Se crea "explotando" la lista de categorías para obtener valores únicos.
print("\n2.1. Creando 'dim_category'...")
dim_category = df.select(explode("categories").alias("category_name")) \
                 .filter(col("category_name").isNotNull()) \
                 .dropDuplicates()
dim_category = dim_category.withColumn("category_id", monotonically_increasing_id())
dim_category.printSchema()
dim_category.show(5, truncate=False)

# 2.2. Dimensión de Características: dim_feature
# Mismo proceso, pero para la lista de características.
print("\n2.2. Creando 'dim_feature'...")
dim_feature = df.select(explode("features").alias("feature_description")) \
                .filter(col("feature_description").isNotNull()) \
                .dropDuplicates()
dim_feature = dim_feature.withColumn("feature_id", monotonically_increasing_id())
dim_feature.printSchema()
dim_feature.show(5, truncate=False)

# 2.3. Dimensión de Imágenes: dim_image
# Se explota la lista y luego se seleccionan los campos de la estructura.
print("\n2.3. Creando 'dim_image'...")
dim_image = df.select(explode("images").alias("image_struct")) \
              .select(
                  col("image_struct.thumb").alias("thumb_url"),
                  col("image_struct.large").alias("large_url"),
                  col("image_struct.hi_res").alias("hi_res_url"),
                  col("image_struct.variant")
              ) \
              .filter(col("large_url").isNotNull()) \
              .dropDuplicates(["thumb_url", "large_url", "hi_res_url", "variant"])
dim_image = dim_image.withColumn("image_id", monotonically_increasing_id())
dim_image.printSchema()
dim_image.show(5, truncate=False)


# ==============================================================================
# PARTE 3: CREACIÓN DE LA DIMENSIÓN CLAVE-VALOR PARA `details`
# ==============================================================================
print("\n--- Parte 3: Creando la Dimensión Flexible para Atributos ('details') ---")

# 3.1. Dimensión de Atributos: dim_product_attribute
# Esta es la solución robusta para manejar la estructura variable de 'details'.
print("\n3.1. Creando 'dim_product_attribute' (Clave-Valor)...")
# Convertimos la estructura 'details' a un mapa (diccionario) para poder explotarla.
map_schema = MapType(StringType(), StringType())
details_exploded_df = df.select("parent_asin", explode(from_json(to_json(col("details")), map_schema)) \
                                .alias("attribute_name", "attribute_value")) \
                         .filter(col("attribute_value").isNotNull())

# Unimos con dim_product para obtener el product_id correcto
dim_product_attribute = details_exploded_df.join(dim_product, on="parent_asin", how="inner") \
                                           .select("product_id", "attribute_name", "attribute_value")
dim_product_attribute.printSchema()
dim_product_attribute.show(10, truncate=False)

print("\n--- Proceso de creación de dimensiones finalizado. ---")


# 
# ## Definición tabla de hechos

# In[6]:


print("\n--- Iniciando creación de Tabla de Hechos y Tablas Puente ---")

# ==============================================================================
# PARTE 4: CREACIÓN DE LA TABLA DE HECHOS
# ==============================================================================
print("\n--- Parte 4: Creando la Tabla de Hechos Central ---")

# 4.1. Tabla de Hechos: fact_product_snapshot
# Unimos el DF original con las dimensiones para obtener las FKs y las métricas.
print("\n4.1. Creando 'fact_product_snapshot'...")
fact_product_snapshot = df.join(dim_product, on="parent_asin", how="left") \
                          .join(dim_store, df.store == dim_store.store_name, how="left") \
                          .select(
                              col("product_id"),
                              col("store_id"),
                              col("price"),
                              col("average_rating"),
                              col("rating_number")
                          ).filter(col("product_id").isNotNull())

fact_product_snapshot.printSchema()
fact_product_snapshot.show(5)

# ==============================================================================
# PARTE 5: CREACIÓN DE LAS TABLAS PUENTE (BRIDGE)
# ==============================================================================
print("\n--- Parte 5: Creando las Tablas Puente para relaciones M:M ---")

# 5.1. Puente Producto-Categoría
print("\n5.1. Creando 'bridge_product_category'...")
bridge_product_category = df.select("parent_asin", explode("categories").alias("category_name")) \
    .join(dim_product, on="parent_asin", how="inner") \
    .join(dim_category, on="category_name", how="inner") \
    .select("product_id", "category_id").dropDuplicates()
bridge_product_category.printSchema()
bridge_product_category.show(5)

# 5.2. Puente Producto-Característica
print("\n5.2. Creando 'bridge_product_feature'...")
bridge_product_feature = df.select("parent_asin", explode("features").alias("feature_description")) \
    .join(dim_product, on="parent_asin", how="inner") \
    .join(dim_feature, on="feature_description", how="inner") \
    .select("product_id", "feature_id").dropDuplicates()
bridge_product_feature.printSchema()
bridge_product_feature.show(5)

# 5.3. Puente Producto-Imagen
print("\n5.3. Creando 'bridge_product_image'...")
# Usamos un alias en el df original para evitar ambigüedad en la condición del join
df_aliased = df.alias("df_aliased")
bridge_product_image = df_aliased.select("parent_asin", explode("images").alias("image_struct")) \
    .join(dim_product, on="parent_asin", how="inner") \
    .join(dim_image, (col("image_struct.large") == dim_image.large_url), how="inner") \
    .select("product_id", "image_id").dropDuplicates()
bridge_product_image.printSchema()
bridge_product_image.show(5)


# In[7]:


# ==============================================================================
# PARTE 6: LIMPIEZA
# ==============================================================================
# Liberar el DataFrame de la memoria caché al finalizar todo el proceso
df.unpersist()

print("\n\nProceso de modelado dimensional finalizado con éxito.")


# ## Almacenar datos en capa Gold

# In[8]:


# Función que hace el insert de los datos en BigQuery
def insert_df(df, table_name: str):
    try:
        table_id = f"{gold_database}.{gold_schema}.{table_name}"
        print(f"\n-> Iniciando la ingesta de datos en la tabla: {table_id}")

        # OPTIMIZACIÓN: Reparticionar el DataFrame antes de escribir.
        # Usa un número razonable de particiones. 200 es un buen punto de partida genérico.
        # Si el DataFrame es pequeño, esto podría ser excesivo, pero para los grandes es vital.
        df_repartitioned = df.repartition(200)

        (df_repartitioned.write
           .format("bigquery")
           .option("table", table_id)
           .option("temporaryGcsBucket", gold_bucket)
           .mode("overwrite")
           .save()
        )
        # CORRECCIÓN DEL PRINT: Usa la variable 'gold_schema', no un DataFrame.
        print(f"   -- Éxito: La tabla '{table_name}' ha sido creada/sobrescrita en el dataset '{gold_schema}'.")

    except Exception as e:
        print(f"   -- ERROR al insertar la tabla '{table_name}': {e}")
        # Detener el bucle si ocurre un error para no intentar más inserciones
        raise e


# In[9]:


# Insert tablas dimensionales
insert_df(dim_product, "dim_product")
insert_df(dim_store, "dim_store_product")
insert_df(dim_category, "dim_category_product")
insert_df(dim_feature, "dim_feature_product")
insert_df(dim_image, "dim_image_product")
insert_df(dim_product_attribute, "dim_attribute_product")

# Insert tabla hechos
insert_df(fact_product_snapshot, "fact_product")

# Tablas puente
insert_df(bridge_product_category, "bridge_product_category")
insert_df(bridge_product_feature, "bridge_product_feature")
insert_df(bridge_product_image, "bridge_product_image")


# ## Detener SparkSession

# In[11]:


# Detener la SparkSession
spark.stop()

