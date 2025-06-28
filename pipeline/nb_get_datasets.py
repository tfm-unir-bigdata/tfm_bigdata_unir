#!/usr/bin/env python
# coding: utf-8

# # **Universidad Internacional de La Rioja**
# ## Escuela Superior de Ingeniería y Tecnología
# ### Máster Universitario en Análisis y Visualización de Datos Masivos / Visual Analytics and Big Data
# 
# ### **Trabajo Final de Máster**
# #### Presentado por:
# - Cepeda Ramos, Jefferson
# - Mosquera Arce, Samek Fernando 

# ---
# ## **Objetivo del notebook: Captura y Almacenamiento de Datos desde Amazon Reviews 2023
# 
# Este proceso tiene como objetivo capturar datos desde la fuente pública de **Amazon Reviews 2023** y almacenarlos en la **capa Bronze** del data lake (`bucket: lk_bronze`) en formato **Parquet**. La fuente proporciona los archivos en formato comprimido `.json.gz`, que serán procesados y transformados de manera controlada y estructurada.
# 
# El flujo general del proceso contempla los siguientes pasos:
# 
# 1. **Insertar Librerías**  
#    Se instalan las dependencias necesarias para el consumo de datos, como `requests`, `pandas`, `pyarrow` y/o `polars`, según se requiera para la lectura, transformación y almacenamiento.
# 
# 2. **Definir Parámetros Base**  
#    Se configuran los parámetros principales, como la URL base del repositorio, las categorías a descargar y la ruta de almacenamiento temporal y final. Esta configuración permite modularidad y escalabilidad del proceso.
# 
# 3. **Consumir Datos vía HTTP**  
#    Se realiza una solicitud HTTP (`requests.get`) para descargar los archivos `.json.gz` correspondientes a cada categoría desde la URL del repositorio oficial de Amazon Reviews.
# 
# 4. **Procesar y Almacenar como Parquet**  
#    Una vez descargados, los archivos `.json.gz` se descomprimen y leen temporalmente para su transformación. Finalmente, se almacenan de forma independiente como archivos `.parquet` en la ruta:  `gs://lk_bronze/GSC/ ` +  `categoría `
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
from pyspark.sql.functions import col, lit
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, ArrayType, MapType, BooleanType
import os
import requests
from google.cloud import storage
import json


# ## Definir Parámetros Base

# In[3]:


# Definir los enlaces a los datasets
# Los datasets de reseñas (reviews)
review_datasets = {
    "Software": "https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/Software.jsonl.gz",
    "Musical_Instruments": "https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/Musical_Instruments.jsonl.gz",
    "Video_Games": "https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/Video_Games.jsonl.gz"
}

# Los datasets de metadatos (meta)
products_datasets = {
    "Software": "https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/meta_categories/meta_Software.jsonl.gz",
    "Musical_Instruments": "https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/meta_categories/meta_Musical_Instruments.jsonl.gz",
    "Video_Games": "https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/meta_categories/meta_Video_Games.jsonl.gz"
}

data_type_products = "products"
data_type_reviews = "reviews"

# Definir la ruta de salida en el bucket de GCS
gcs_output_path = "gs://lk_bronze/GSC/"

# Definir el esquema explícito para los datasets de metadatos (productos)
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
        StructField("user_id", StringType(), True) # Añadido 'user_id' según el ejemplo de JSON
    ])), True),
    StructField("store", StringType(), True),
    StructField("categories", ArrayType(StringType()), True),
    StructField("details", StructType([
        StructField("release_date", StringType(), True), # Original: "Release Date"
        StructField("date_first_listed_on_amazon", StringType(), True), # Original: "Date first listed on Amazon"
        StructField("developed_by", StringType(), True), # Original: "Developed By"
        StructField("size", StringType(), True), # Original: "Size"
        StructField("version", StringType(), True), # Original: "Version"
        StructField("application_permissions", ArrayType(StringType()), True), # Original: "Application Permissions"
        StructField("minimum_operating_system", StringType(), True), # Original: "Minimum Operating System"
        StructField("approximate_download_time", StringType(), True) # Original: "Approximate Download Time"
    ]), True),
    StructField("parent_asin", StringType(), True),
    StructField("bought_together", ArrayType(StringType()), True)
])

# Definir el esquema para los datasets de reseñas (reviews)
reviews_schema = StructType([
    StructField("rating", FloatType(), True),
    StructField("title", StringType(), True),
    StructField("text", StringType(), True),
    StructField("images", ArrayType(StructType([
        StructField("small_image_url", StringType(), True),
        StructField("medium_image_url", StringType(), True),
        StructField("large_image_url", StringType(), True)
    ])), True),
    StructField("asin", StringType(), True),
    StructField("parent_asin", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("verified_purchase", BooleanType(), True),
    StructField("helpful_vote", LongType(), True)
])

# Inicializar cliente de Google Cloud Storage
# Las credenciales se obtienen automáticamente en el entorno de Dataproc
storage_client = storage.Client()


# ## Normalizar archivos JSONL

# In[4]:


# Función auxiliar para normalizar claves JSON, ajustada para el schema de products
def normalize_json_keys_products(json_string):
    """
    Parsea una cadena JSON, normaliza sus claves y construye un objeto Row que
    coincide estrictamente con el 'product_schema' definido.
    Maneja la normalización de claves en top-level y dentro de 'details',
    y asegura la compatibilidad de tipos para la creación del DataFrame.
    """
    try:
        record = json.loads(json_string)
        final_record_for_row = {}

        # Iterar sobre los campos definidos en el product_schema
        for field_in_schema in product_schema.fields:
            field_name_in_schema = field_in_schema.name # Nombre del campo en el esquema (ej: 'main_category', 'details')

            # Valor a asignar al campo en el Row, por defecto None
            value_for_row_field = None

            # Normalizar las claves de top-level para buscar en el JSON original
            # Convertir el nombre del campo del esquema a posibles formatos de clave en el JSON original
            possible_json_keys = [field_name_in_schema] # snake_case
            if '_' in field_name_in_schema:
                possible_json_keys.append(field_name_in_schema.replace('_', ' ').title()) # Title Case with spaces
                possible_json_keys.append(field_name_in_schema.replace('_', ' ').lower())  # lower case with spaces
                # Add any other common variations if needed based on data

            # Buscar el valor en el nivel superior del registro JSON
            for key_variant in possible_json_keys:
                if key_variant in record:
                    value_for_row_field = record[key_variant]
                    break

            # Manejo especial para el campo 'details' (que es un StructType anidado)
            if field_name_in_schema == "details":
                processed_details = {}
                if isinstance(value_for_row_field, dict):
                    # Iterar sobre los sub-campos definidos en el esquema de 'details'
                    for detail_sub_field in field_in_schema.dataType.fields:
                        sub_field_name_in_schema = detail_sub_field.name # ej: 'release_date'

                        # Normalizar las claves de los sub-campos para buscar en el diccionario 'details'
                        possible_detail_json_keys = [sub_field_name_in_schema]
                        if '_' in sub_field_name_in_schema:
                            possible_detail_json_keys.append(sub_field_name_in_schema.replace('_', ' ').title())
                            possible_detail_json_keys.append(sub_field_name_in_schema.replace('_', ' ').lower())

                        detail_sub_field_value = None
                        for detail_key_variant in possible_detail_json_keys:
                            if detail_key_variant in value_for_row_field:
                                detail_sub_field_value = value_for_row_field[detail_key_variant]
                                break

                        # Manejo específico para 'application_permissions' (ArrayType) dentro de 'details'
                        if sub_field_name_in_schema == "application_permissions" and detail_sub_field_value is not None:
                            if isinstance(detail_sub_field_value, str):
                                processed_details[sub_field_name_in_schema] = [detail_sub_field_value] # Wrap string in list
                            elif isinstance(detail_sub_field_value, list):
                                processed_details[sub_field_name_in_schema] = [str(item) for item in detail_sub_field_value if item is not None]
                            else:
                                processed_details[sub_field_name_in_schema] = None
                        else:
                            processed_details[sub_field_name_in_schema] = detail_sub_field_value

                value_for_row_field = processed_details

            # Manejo para campos que son ArrayType (listas) en el esquema
            elif isinstance(field_in_schema.dataType, ArrayType):
                if value_for_row_field is not None:
                    if isinstance(value_for_row_field, str): # Si es una cadena, conviértela a una lista de cadenas
                        value_for_row_field = [value_for_row_field]
                    elif not isinstance(value_for_row_field, list): # Si no es una lista ni una cadena, ponlo como None
                        value_for_row_field = None
                    else: # Si es una lista, asegúrate de que los elementos sean del tipo esperado (ej: StringType)
                        # Para listas de structs (images, videos), es necesario un procesamiento más profundo
                        if isinstance(field_in_schema.dataType.elementType, StructType):
                            processed_list = []
                            for item in value_for_row_field:
                                if isinstance(item, dict):
                                    # Para 'images' o 'videos', asegurar que las claves son las que espera el esquema
                                    item_processed = {}
                                    for sub_struct_field in field_in_schema.dataType.elementType.fields:
                                        sub_struct_field_name = sub_struct_field.name
                                        item_processed[sub_struct_field_name] = item.get(sub_struct_field_name)
                                    processed_list.append(item_processed)
                                else:
                                    processed_list.append(None) # O un diccionario vacío si aplica
                            value_for_row_field = processed_list
                        else: # Para listas de tipos simples (StringType)
                            value_for_row_field = [str(item) for item in value_for_row_field if item is not None]
                else: # Si value_for_row_field es None
                    value_for_row_field = [] # Default to empty list if nullable and not present

            # Manejo para price (StringType en esquema, puede ser float en JSON)
            elif field_name_in_schema == "price" and value_for_row_field is not None:
                value_for_row_field = str(value_for_row_field) # Convertir a string para el esquema

            # Asignar el valor final al registro que se convertirá en Row
            final_record_for_row[field_name_in_schema] = value_for_row_field

        # Crear el objeto Row con los datos que coinciden exactamente con el esquema
        return Row(**final_record_for_row)
    except json.JSONDecodeError as e:
        print(f"Advertencia: Línea JSON malformada detectada y omitida: {json_string[:100]}... Error: {e}")
        return None
    except Exception as e:
        print(f"Error inesperado al normalizar claves: {e} para línea: {json_string[:100]}...")
        return None


# In[5]:


# Función auxiliar para normalizar claves JSON, ajustada para el schema de reviews
def normalize_json_keys_reviews(json_string):
    """
    Parsea una cadena JSON, normaliza sus claves y construye un objeto Row que
    coincide estrictamente con el 'reviews_schema' definido.
    """
    try:
        record = json.loads(json_string)
        final_record_for_row = {field.name: None for field in reviews_schema.fields}

        for field_in_schema in reviews_schema.fields:
            field_name_in_schema = field_in_schema.name
            value_found = record.get(field_name_in_schema)

            # Manejo para campos ArrayType (listas)
            if isinstance(field_in_schema.dataType, ArrayType):
                if value_found is not None:
                    if isinstance(field_in_schema.dataType.elementType, StructType):
                        processed_list = []
                        for item in value_found:
                            if isinstance(item, dict):
                                item_processed = {}
                                for sub_struct_field in field_in_schema.dataType.elementType.fields:
                                    sub_struct_field_name = sub_struct_field.name
                                    item_processed[sub_struct_field_name] = item.get(sub_struct_field_name)
                                processed_list.append(item_processed)
                            else:
                                processed_list.append(None)
                        final_record_for_row[field_name_in_schema] = processed_list
                    else: # Para ArrayType de tipos simples (ej: StringType)
                        final_record_for_row[field_name_in_schema] = [str(item) for item in value_found if item is not None]
                else:
                    final_record_for_row[field_name_in_schema] = [] # Por defecto, lista vacía para arrays nulos

            # Manejo para FloatType (rating)
            elif field_in_schema.dataType == FloatType():
                try:
                    final_record_for_row[field_name_in_schema] = float(value_found) if value_found is not None else None
                except (ValueError, TypeError):
                    final_record_for_row[field_name_in_schema] = None

            # Manejo para LongType (timestamp, helpful_vote)
            elif field_in_schema.dataType == LongType():
                try:
                    final_record_for_row[field_name_in_schema] = int(value_found) if value_found is not None else None
                except (ValueError, TypeError):
                    final_record_for_row[field_name_in_schema] = None

            # Manejo para BooleanType (verified_purchase)
            elif field_in_schema.dataType == BooleanType():
                if value_found is True or value_found is False:
                    final_record_for_row[field_name_in_schema] = value_found
                elif isinstance(value_found, str) and value_found.lower() in ['true', 'false']:
                    final_record_for_row[field_name_in_schema] = (value_found.lower() == 'true')
                else:
                    final_record_for_row[field_name_in_schema] = None

            # Para StringType y otros tipos simples que no tienen manejo especial
            else:
                if value_found is not None:
                    final_record_for_row[field_name_in_schema] = str(value_found)
                else:
                    final_record_for_row[field_name_in_schema] = None

        return Row(**final_record_for_row)
    except json.JSONDecodeError as e:
        print(f"Advertencia (Reviews): Línea JSON malformada detectada y omitida: {json_string[:100]}... Error: {e}")
        return None
    except Exception as e:
        print(f"Error inesperado al normalizar claves (Reviews): {e} para línea: {json_string[:100]}... Tipo de error: {type(e).__name__}")
        return None


# ## Procesar consumo vía HTTP y Almacenar como Parquet

# In[6]:


def process_and_save_dataset(name, url, data_type, schema=None):
    print(f"\nProcesando {data_type} para la categoría: {name} desde {url}")

    # Extraer el nombre del bucket de la ruta gcs_output_path
    # gcs_output_path es como gs://nombre_bucket/ruta/
    bucket_name = gcs_output_path.split("//")[1].split("/")[0]

    # Crear una ruta temporal en GCS para el archivo .gz descargado
    # Ejemplo: lk_bronze/temp_downloads/software_products.jsonl.gz
    temp_gcs_filename = f"temp_downloads/{name.lower()}_{data_type}.jsonl.gz"
    temp_gcs_path_full = f"gs://{bucket_name}/{temp_gcs_filename}"

    blob = None # Inicializar blob fuera del try para que sea accesible en finally
    try:
        # 1. Descargar el archivo .gz de la URL
        print(f"Descargando {url} a una ubicación temporal en GCS...")
        response = requests.get(url, stream=True)
        response.raise_for_status() # Lanza un HTTPError para respuestas de error (4xx o 5xx)

        # 2. Subir el contenido descargado directamente a GCS
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(temp_gcs_filename)

        # Abrir el blob en modo escritura binaria y escribir los chunks directamente
        with blob.open("wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Archivo descargado y subido a GCS temporalmente: {temp_gcs_path_full}")

        # 3. Extraer la data del archivo .jsonl.gz desde la ubicación temporal en GCS
        # Leer como texto y luego parsear para manejar duplicados de columnas
        rdd = spark.sparkContext.textFile(temp_gcs_path_full)

        # Mapear el RDD de cadenas a un RDD de objetos Row con claves normalizadas
        # Filtrar las líneas vacías y las que no se pudieron parsear
        # La función normalize_json_keys ahora está diseñada para usar el product_schema global
        if data_type == "products":
            rows = rdd.filter(lambda x: x is not None and x.strip() != "").map(normalize_json_keys_products).filter(lambda x: x is not None)
        else:
            rows = rdd.filter(lambda x: x is not None and x.strip() != "").map(normalize_json_keys_reviews).filter(lambda x: x is not None)

        # Crear un DataFrame de Spark a partir del RDD de Rows, aplicando el esquema explícito
        # El esquema se pasa como argumento, pero la función normalize_json_keys ya lo usa internamente    
        df = spark.createDataFrame(rows, schema=schema)

        print(f"Número de registros leídos para {name} ({data_type}): {df.count()}")

        # Agregar columnas para identificar la categoría y el tipo de dato
        df = df.withColumn("category", lit(name))

        # 4. Convertir la data a formato Parquet y guardarla en GCS
        # La ruta final será gs://lk_bronze/GSC/<data_type>/<nombre_categoria>/
        output_dir = os.path.join(f"{gcs_output_path}{data_type}", name.lower())

        # Usar modo 'overwrite' para reemplazar si ya existe, o 'append' para añadir
        df.write.mode("overwrite").parquet(output_dir)
        print(f"Datos de {name} ({data_type}) guardados exitosamente en Parquet en: {output_dir}")

    except Exception as e:
        print(f"Error al procesar {name} ({data_type}): {e}")
    finally:
        # 5. Eliminar el archivo temporal de GCS
        if blob: # Solo intentar eliminar si el blob fue creado
            try:
                blob.delete()
                print(f"Archivo temporal {temp_gcs_path_full} eliminado de GCS.\n")
            except Exception as e:
                print(f"Advertencia: No se pudo eliminar el archivo temporal {temp_gcs_path_full}: {e}\n")


# ## **Iniciar procesamiento**

# ### 1. Datasets products

# In[20]:


# Procesar los datasets de metadatos products
for name, url in products_datasets.items():
    process_and_save_dataset(name, url, data_type_products, schema=product_schema)

print("\nProceso completado para todos los datasets de products meta.")


# ### 2. Datasets reviews

# In[7]:


# Procesar los datasets de reviews
for name, url in review_datasets.items():
    process_and_save_dataset(name, url, data_type_reviews, schema=reviews_schema)

print("\nProceso completado para todos los datasets de reviews.")


# ## Detener SparkSession

# In[ ]:


# Detener la SparkSession
spark.stop()

