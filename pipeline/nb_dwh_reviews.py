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
# ## **Objetivo del notebook: Proceso de ingesta de datos datasets reseñas en la capa Gold**
# 
# Este proceso tiene como objetivo realizarla ingesta de los datos de reseñas de usuarios almacenados en la **capa Silver** en GCP (`gs://lk_silver/GSC/reviews/`) en la **capa Gold** en Google BigQuery, en el schema `dw_gold`.
# 
# El flujo general del proceso contempla los siguientes pasos:
# 
# 1.  **Lectura de los datos**:
#     * Leer el dataset de reseñas desde la capa Silver.
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
from pyspark.sql.functions import col, row_number, monotonically_increasing_id


# ## Definir Parámetros Base

# In[3]:


# Definir el esquema explícito para los datasets de metadatos (productos)
# Rutas del datasets en GCP

# Ruta origen del dataset en la capa Silver
silver_path = "gs://lk_silver/GSC/reviews/"

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

# In[9]:


print("\nInicia el proceso de definición de las tablas dimensionales")

# dimensión categoría
print("\n1.1. Creando 'dim_category'...")
dim_category = df.select("category").dropDuplicates()
dim_category = dim_category.withColumn("category_id", monotonically_increasing_id())
dim_category.show()

# dimensión compras verificadas
print("\n1.2. Creando 'dim_verified_purchase'...")
dim_verified_purchase = df.select("verified_purchase").dropDuplicates()
dim_verified_purchase = dim_verified_purchase.withColumn("verified_purchase_id", monotonically_increasing_id())
dim_verified_purchase.show()


# 
# ## Definición tabla de hechos

# In[10]:


print("\nInicia el proceso de definición de la tabla de hechos")
# Define el df como fact
fact = df

# Agregar 'category_id' a la tabla fact
fact = fact.join(
    dim_category.select("category", "category_id"),
    on="category",
    how="left"
)

# Agregar 'verified_purchase_id' a la tabla fact
fact = fact.join(
    dim_verified_purchase.select("verified_purchase", "verified_purchase_id"),
    on="verified_purchase",
    how="left"
)

# Eliminar columnas no necesarias
fact = fact.drop("category", "verified_purchase")

# Define el orden de las columnas para la ingesta en BigQuery
column_order = [
    "user_id",
    "asin",
    "parent_asin",
    "datetime",
    "title",
    "review",
    "rating",
    "helpful_vote",
    "category_id",
    "verified_purchase_id"
]

# Reordena las columnas
fact = fact.select(*column_order)

print("\nEsquema de la tabla de hechos:")
fact.printSchema()


# ## Almacenar datos en capa Gold

# In[11]:


# Función que hace el insert de los datos en BigQuery
def insert_df(df, table_name: str):

    # Construimos el identificador completo de la tabla
    table_id = f"{gold_database}.{gold_schema}.{table_name}"

    # 2. Proceso de escritura en BigQuery
    print(f"\nIniciando la ingesta de datos en la tabla: {table_id}")
    print(f"Modo de escritura: overwrite")
    print(f"Bucket temporal: {gold_bucket}")

    # Usamos el DataFrameWriter para configurar y ejecutar la operación
    (df.write
      .format("bigquery")
      .option("table", table_id)
      .option("temporaryGcsBucket", gold_bucket)
      .mode("overwrite")
      .save()
    )

    print(f"La tabla '{table_name}' ha sido creada/sobrescrita en el dataset '{dim_category}'.")


# In[12]:


# Insert tablas dimensionales
insert_df(dim_category, "dim_category")
insert_df(dim_verified_purchase, "dim_verified_purchase")

# Insert tabla hechos
insert_df(fact, "fact_reviews")

print("\n Proceso de insert de datos completado.")


# ## Detener SparkSession

# In[ ]:


# Detener la SparkSession
spark.stop()

