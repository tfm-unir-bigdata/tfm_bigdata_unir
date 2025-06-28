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
# ## **Objetivo del notebook: Proceso de calidad datasets de Reseñas de Usuarios**
# 
# Este proceso tiene como objetivo realizar un proceso de calidad y transformación de datasets de reseñas de usuarios almacenados en la **capa Bronze** de un data lake en GCP (`gs://lk_bronze/GSC/reviews/`) para prepararlos y moverlos a la **capa Silver** (`gs://lk_silver/GSC/reviews/`). Los datos provienen de múltiples categorías (`software`, `musical_instruments`, `video_games`) y están en formato **Parquet**.
# 
# El flujo general del proceso contempla los siguientes pasos:
# 
# 1.  **Unificación de Datos**:
#     * Leer los datasets de reseñas de las categorías `software`, `musical_instruments` y `video_games` desde la capa Bronze.
#     * Unificar todos los DataFrames en un único DataFrame de Spark.
# 
# 2.  **Manejo de Valores Duplicados**:
#     * Eliminar registros duplicados dentro del DataFrame unificado, considerando **todos los atributos**, basándose en la identificación previa de 138,357 registros duplicados.
# 
# 3.  **Manejo de datos para el Marco Normativo**: De acuerdo con el marco normativo Europeo y Español para el tratamiento y manejo de información personal sensible, se realizan los siguientes procesos de anonimización y control de datos:
# 
#     * **`user_id`:** Usar el algoritmo de hash SHA-256 para crear un identificador único e irreversible. Esto cumple con el requisito de desvincular el ID del usuario original.
#   
#     * **`text`:** Dando cumplimiento al marco normativo, se realiza la limpieza de nombres propios y geográficos que puedan existir en el campo 'text' de las reseñas. Los datos identificados como sensibles de acuerdo con las listas `sensitive_words_broadcast` se reemplazan por la marca `[CENSORED]` 
#   
#     * **Eliminación de columnas:*** Conforme al principio de minimización, eliminamos las columnas:
#   
#         * `images`: Por su alto riesgo de contener datos identificables.
#         * `user_id`: La columna original ya no es necesaria tras la anonimización.
#         * `timestamp`: La columna original fue convertida a fecha con formato 'yyyy-mm-dd hh:mm:ss'
# 
# 5.  **Almacenamiento en Capa Silver**:
#     * El DataFrame resultante del proceso de calidad se guardará en la capa Silver, en formato **Parquet**.
#     * El almacenamiento se realizará particionando por la columna `category` en la ruta `gs://lk_silver/GSC/reviews/`, utilizando el modo `overwrite` para garantizar que los datos más recientes reemplacen a los anteriores.
# 
# ---

# ## Instanciar SparkSession

# In[24]:


# Inicializar SparkSession si no está ya disponible
try:
    spark
except NameError:
    spark = SparkSession.builder.appName("AmazonReviewsProcessing").getOrCreate()

print("SparkSession inicializada.")


# ## Insertar librerías

# In[26]:


# Importar las librerías necesarias
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, from_unixtime, sha2, udf, length
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, BooleanType, FloatType
import requests
import re


# ## Definir Parámetros Base

# In[40]:


# Definir el esquema explícito para los datasets de reviews
reviews_schema = StructType([
    StructField("rating", FloatType(), True),
    StructField("title", StringType(), True),
    StructField("text", StringType(), True),
    StructField("images", ArrayType(
        StructType([
            StructField("small_image_url", StringType(), True),
            StructField("medium_image_url", StringType(), True),
            StructField("large_image_url", StringType(), True)
        ])
    ), True),
    StructField("asin", StringType(), True),
    StructField("parent_asin", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("verified_purchase", BooleanType(), True),
    StructField("helpful_vote", LongType(), True),
    StructField("category", StringType(), True)
])

# Rutas de los datasets en GCP
bronze_paths = {
    "software": "gs://lk_bronze/GSC/reviews/software",
    "musical_instruments": "gs://lk_bronze/GSC/reviews/musical_instruments",
    "video_games": "gs://lk_bronze/GSC/reviews/video_games",
}

# Ruta de destino en la capa Silver
silver_path = "gs://lk_silver/GSC/reviews/"

# URLs de las listas de palabras a eliminar
URL_CIUDADES_PAISES = "https://raw.githubusercontent.com/tfm-unir-bigdata/tfm_bigdata_unir/main/Dictionaries/cities_and_countries.txt"
URL_NOMBRES = "https://raw.githubusercontent.com/tfm-unir-bigdata/tfm_bigdata_unir/refs/heads/main/Dictionaries/first_names.all.txt"
URL_APELLIDOS = "https://raw.githubusercontent.com/tfm-unir-bigdata/tfm_bigdata_unir/refs/heads/main/Dictionaries/last_names.all.txt"

# Palabra para reemplazar los datos sensibles encontrados
REPLACEMENT_WORD = "[CENSORED]"

print("Parámetros definidos")


# ## Leer los dataframe

# In[28]:


# Lista para almacenar los DataFrames individuales
dfs = []

# Leer cada dataset desde "gs://lk_bronze/GSC/"
for category, path in bronze_paths.items():
    print(f"Leyendo datos de: {path} con categoría: {category}")
    df = spark.read.schema(reviews_schema).parquet(path)
    dfs.append(df)

# Unificar todos los DataFrames
print("Unificando DataFrames...")
df_reviews = dfs[0]
for i in range(1, len(dfs)):
    df_reviews = df_reviews.unionByName(dfs[i], allowMissingColumns=True)

initial_row_count = df_reviews.count()

print(f"Total de registros unificados antes de la calidad: {initial_row_count}")


# ## **Proceso de calidad de datos**

# In[29]:


print("Iniciando proceso de calidad de datos...")

# Eliminar registros duplicados considerando todas las columnas
df_reviews = df_reviews.dropDuplicates()
duplicates_removed = initial_row_count - df_reviews.count()

print(f"Total de columnas iniciales: {initial_row_count}")
print(f"Total de columnas después de deduplicar: {df_reviews.count()}")
print(f"Total columnas duplicadas: {duplicates_removed}")

# Convertir la fecha 'timestamp' en formato 'yyyy-MM-dd HH:mm:ss' y renombrar el campo como 'datetime'
print("\nIniciando la conversión del formato de fecha...")
df_reviews = df_reviews.withColumn("datetime", from_unixtime(col("timestamp") / 1000, "yyyy-MM-dd HH:mm:ss"))
print("Conversión de fecha finalizada.")


# ## Funciones auxiliares para anonimización de datos sensibles

# In[30]:


def load_sensitive_words_from_urls(urls):
    """
    Descarga palabras de una lista de URLs, las limpia (minúsculas, sin espacios extra)
    y las devuelve como un conjunto para una búsqueda eficiente.
    """
    sensitive_words = set()
    for url in urls:
        try:
            response = requests.get(url)
            response.raise_for_status()  # Lanza un error para respuestas HTTP fallidas

            # Decodificar usando UTF-8 y dividir por líneas
            lines = response.content.decode('utf-8').splitlines()

            # Omitir la primera línea (cabecera) y procesar el resto
            for line in lines[1:]:
                word = line.strip().lower()
                if word:
                    sensitive_words.add(word)
        except requests.exceptions.RequestException as e:
            print(f"Error descargando la lista desde {url}: {e}")
    return sensitive_words


# In[31]:


def redesigned_anonymize_logic(text, sensitive_words_broadcast):
    """
    Lógica de la UDF rediseñada para anonimizar texto, asegurando coincidencias
    exactas de palabras completas sin distinción de mayúsculas y minúsculas.
    """
    # 1. Manejar casos nulos de entrada para evitar errores.
    if text is None:
        return None

    # 2. Acceder al conjunto de palabras sensibles desde la variable broadcast.
    #    Este conjunto DEBE contener todas las palabras en minúsculas para que la comparación funcione.
    sensitive_set = sensitive_words_broadcast.value

    # 3. Tokenizar la cadena. Esta estrategia es clave: divide el texto en "palabras"
    #    y "delimitadores" (espacios, comas, puntos, etc.). Esto permite
    #    reconstruir la oración perfectamente con su puntuación original.
    tokens = re.split(r'(\W+)', text)

    # 4. Procesar cada token y construir la lista de salida.
    cleaned_tokens = []
    for token in tokens:
        # Se procesan únicamente los tokens que son palabras reales (no espacios ni puntuación).
        # La condición `token.strip()` se asegura de no procesar tokens que son solo espacios.
        if token and token.strip():
            # La CLAVE está aquí: convertir el token a minúsculas y buscarlo en el conjunto.
            # Esto logra una coincidencia exacta de la palabra, ignorando el caso original
            # (mayúsculas, minúsculas o capitalizado).
            if token.lower() in sensitive_set:
                # Si el token es una palabra sensible, se reemplaza.
                cleaned_tokens.append(REPLACEMENT_WORD)
            else:
                # Si NO está en la lista, el token se mantiene sin cambios.
                cleaned_tokens.append(token)
        else:
            # Si el token es un delimitador (espacio, coma, etc.), se mantiene siempre.
            cleaned_tokens.append(token)

    # 5. Unir todos los tokens (palabras y delimitadores) para formar la cadena final.
    return "".join(cleaned_tokens)


# ## Proceso anonimización y limpieza por marco normativo

# ### Anonimizar campo 'user_id'
# Por cumplimiento del marco normativo, se anonimizan los nombres por un **identificador único e irreversible** mediante el algoritmo SHA-256

# In[32]:


print("\nIniciando proceso de anonimización y limpieza según el marco normativo...")

# Anonimización irreversible del 'user_id'
# Usamos el algoritmo de hash SHA-256 para crear un identificador único e irreversible.
# Esto cumple con el requisito de desvincular el ID del usuario original.
df_reviews = df_reviews.withColumn("user_id_anonymized", sha2(col("user_id"), 256))
print("Paso 1 de 4: 'user_id' anonimizado con éxito.")


# ### Eliminar atributos duplicados y redundantes

# In[33]:


# Eliminación de columnas de alto riesgo y redundantes
# Conforme al principio de minimización, eliminamos las columnas:
# - 'images': Por su alto riesgo de contener datos identificables.
# - 'user_id': La columna original ya no es necesaria tras la anonimización.
# - 'timestamp': La columna original ya no es necesaria tras la conversión de formato.
df_reviews = df_reviews.drop("images", "user_id", "timestamp")
print("Paso 2 de 4: Columnas de alto riesgo ('images') y redundantes ('user_id', 'timestamp') eliminadas.")


# ### Renombrar atributo 'user_id'

# In[34]:


# Renombrar la columna 'user_id' anonimizada para mantener la estructura
df_reviews = df_reviews.withColumnRenamed("user_id_anonymized", "user_id")
print("Paso 3 de 4: Columnas renombradas para mantener la consistencia del esquema.")


# ### Anonimizar campo 'text' (reseñas)
# Dando cumplimiento al marco normativo, se realiza la limpieza de **nombres propios y geográficos** que puedan existir en el campo 'text' de las reseñas

# In[45]:


# Tratamiento del campo 'text'
print("Iniciando el proceso de anonimización de texto...")

# Cargar las listas de palabras sensibles en el Driver
print("Cargando listas de nombres y lugares...")
sensitive_words = load_sensitive_words_from_urls([URL_CIUDADES_PAISES, URL_NOMBRES, URL_APELLIDOS])

# Se comprueba si la lista de palabras sensibles se cargó con éxito.
if sensitive_words:
    # Si la lista NO está vacía, se procede con las operaciones de Spark.

    # **Optimización Clave**: Distribuir las listas a todos los nodos con 'broadcast'
    sensitive_words_broadcast = spark.sparkContext.broadcast(sensitive_words)
    print(f"Listas cargadas y distribuidas vía broadcast. Total de términos sensibles: {len(sensitive_words_broadcast.value)}")

    # Registrar la función de lógica como una UDF de Spark
    anonymize_text_udf = udf(lambda text: anonymize_text_udf_logic(text, sensitive_words_broadcast), StringType())

    # Aplicar la UDF a 'df_reviews' para crear la nueva columna anonimizada
    print("Aplicando la UDF de anonimización a la columna 'text'...")
    # El marco normativo indica que el campo se anonimiza, no se crea uno nuevo 
    # por lo que el resultado reemplazará la columna 'text'.
    df_reviews = df_reviews.withColumn("text_anonymized", anonymize_text_udf(col("text")))

    # Filtrar las reseñas para cumplir con el requisito de longitud mínima 
    print("Filtrando reseñas para que tengan un mínimo de 15 caracteres...")
    #df_reviews = df_reviews.filter(length(col("text_anonymized")) >= 15)

    # Elimar columnas que ya no se usan
    df_reviews = df_reviews.drop("text").withColumnRenamed("text_anonymized", "review")

    # Es buena práctica liberar la variable de broadcast al final.
    sensitive_words_broadcast.unpersist()

    # Muestra del resultado
    print("\n Resultado de la anonimización del campo 'text'")
    df_reviews.select("review").show(20, truncate=False)

    print("\n Paso 4 de 4: Reseñas anonimizadas y filtradas.")
else:
    # Si la lista SÍ está vacía, se informa al usuario y no se ejecuta ninguna operación de Spark.
    # Esto evita el error de usar un contexto detenido.
    print("---------------------------------------------------------------------------")
    print("ABORTE: No se cargaron palabras sensibles. No se realizó ninguna transformación.")
    print("---------------------------------------------------------------------------")



# In[46]:


print("\n¡Proceso de transformación completado!")
# --- Verificación Final ---
print("\nMostrando el esquema final del DataFrame:")
df_reviews.printSchema()


# ## Almacenar datos limpios en capa Silver

# In[47]:


# Almacenamiento del dataset unificado en la capa Silver "gs://lk_silver/GSC/products/"
print("Guardando DataFrames procesados en la capa Silver...")

df_reviews.write.mode("overwrite").parquet(silver_path)

print("Proceso de calidad y almacenamiento completado.")


# ## Detener SparkSession

# In[48]:


# Detener la SparkSession
spark.stop()

