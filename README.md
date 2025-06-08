# Universidad Internacional de La Rioja  
## Escuela Superior de Ingeniería y Tecnología  
### Máster Universitario en Análisis y Visualización de Datos Masivos / Visual Analytics and Big Data  

---

## Trabajo final de má́ster:  
**Procesado Masivo para el análisis de reseñas y productos de Amazon**  
**Trabajo fin de estudio presentado por:**  
- Cepeda Ramos, Jefferson  
- Mosquera Arce, Samek Fernando  

**Tipo de trabajo:** Desarrollo Software  
**Director:** Ambiente, Enrique de Miguel  

---

# tfm_bigdata_unir

---

## 📘 Versión en Español

### Descripción  
El proyecto implementa un pipeline ETL escalable para el procesamiento de grandes volúmenes de datos de reseñas y productos de Amazon (McAuley, 2023), cargándolos en un Data Lakehouse optimizado en Google Cloud Platform.

### Objetivo general  
Implementar un pipeline ETL para el procesamiento eficiente y escalable de grandes cantidades de datos, utilizando los conjuntos de datos de reseñas y productos de Amazon como fuente, recolectados por McAuley (Reseñas de Amazon ’23, 2023). Este pipeline llevará a cabo la extracción, transformación, enriquecimiento y carga de datos en un Data Lakehouse optimizado para almacenamiento a gran escala y procesamiento analítico.

### Objetivos específicos  
1. **Selección de subconjuntos**  
   - “Musical_Instruments”: 3 017 438 reseñas, 213 591 productos  
   - “Videogames”: 4 624 614 reseñas, 137 268 productos  
   - “Software”: 4 880 180 reseñas, 89 250 productos  
   (Total: 12 522 232 reseñas, 440 109 productos, 12 962 321 registros).

2. **Arquitectura de datos**  
   Definición de componentes (almacenamiento, procesamiento, integración), flujo de datos y tecnologías (BigQuery, Cloud Storage, Dataflow).

3. **Análisis exploratorio (EDA)**  
   Evaluación de estructura, calidad, patrones y volumen de los datos seleccionados.

4. **Desarrollo del pipeline ETL**  
   Implementación de un workflow gobernado y altamente escalable en GCP, con extracción, transformación y enriquecimiento mediante Dataflow y Python.

5. **Métricas (KPIs)**  
   Definición y seguimiento de indicadores de rendimiento del pipeline: tiempo de procesamiento, coste en la nube, calidad de datos.

6. **Dashboard interactivo**  
   Visualización de KPIs y principales hallazgos en un panel accesible para usuarios de Business Analytics.

### Plataforma y tecnologías  
- **Cloud**: Google Cloud Platform (BigQuery, Cloud Storage, Dataflow, Composer)  
- **Lenguajes**: Python, SQL  
- **Orquestación**: Airflow / Cloud Composer  
- **Visualización**: Data Studio / Looker Studio  

### Estructura del repositorio  
- **data**: Scripts de descarga y preprocesado
- **etl**: Código del pipeline ETL
- **analytics**: Notebooks de EDA
- **dashboard**: Definición del dashboard
- **docs**: Documentación adicional
- **README.md**: Descripción del proyecto

## Referencias
- Hou, Y., Li, J., He, Z., Yan, A., Chen, X. & McAuley, J. (2024). _Bridging Language and Items for Retrieval and Recommendation_. arXiv preprint arXiv:2403.03952.

---

## 📙 English Version

### Description  
This project implements a scalable ETL pipeline to process large volumes of Amazon review and product data (McAuley, 2023), loading them into an optimized Data Lakehouse on Google Cloud Platform.

### General Objective  
Implement an ETL pipeline for the efficient and scalable processing of large amounts of data, using Amazon review and product datasets collected by McAuley (Amazon Reviews ’23, 2023). The pipeline will perform extraction, transformation, enrichment, and loading into a Data Lakehouse optimized for large-scale storage and analytical processing.

### Specific Objectives  
1. **Subset Selection**  
   - “Musical_Instruments”: 3,017,438 reviews, 213,591 products  
   - “Videogames”: 4,624,614 reviews, 137,268 products  
   - “Software”: 4,880,180 reviews, 89,250 products  
   (Total: 12,522,232 reviews, 440,109 products, 12,962,321 records).

2. **Data Architecture**  
   Define data architecture components (storage, processing, integration), data flow, and technologies (BigQuery, Cloud Storage, Dataflow).

3. **Exploratory Data Analysis (EDA)**  
   Assess data structure, quality, patterns, and volume for the selected subsets.

4. **ETL Pipeline Development**  
   Build a governed, highly scalable workflow on GCP using Dataflow and Python.

5. **Key Performance Indicators (KPIs)**  
   Define and monitor pipeline metrics: processing time, cloud cost, data quality.

6. **Interactive Dashboard**  
   Visualize KPIs and key insights in a dashboard for Business Analytics users.

### Platform & Technologies  
- **Cloud**: Google Cloud Platform (BigQuery, Cloud Storage, Dataflow, Composer)  
- **Languages**: Python, SQL  
- **Orchestration**: Airflow / Cloud Composer  
- **Visualization**: Data Studio / Looker Studio  

### Repository Structure
- **data**: Download and preprocessing scripts  
- **etl**: ETL pipeline code  
- **analytics**: EDA notebooks  
- **dashboard**: Dashboard definitions  
- **docs**: Additional documentation  
- **README.md**: Project overview  

## References
- Hou, Y., Li, J., He, Z., Yan, A., Chen, X. & McAuley, J. (2024). _Bridging Language and Items for Retrieval and Recommendation_. arXiv preprint arXiv:2403.03952.
