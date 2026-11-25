import streamlit as st
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from pathlib import Path
from Dashboard import Dashboard
from queries import Queries_SP, Queries_PG, Queries_HDFS, Queries_MG

from Dashboard.Filtros import Filtros

env_path = Path(__file__).with_name(".env")

load_dotenv(dotenv_path=env_path)

HADOOP_URI = os.getenv("HADOOP_URI")
spark_remote = os.getenv("SPARK_URL")
@st.cache_resource
def get_spark() -> SparkSession:
    return SparkSession.builder.remote(spark_remote).getOrCreate()
spark = get_spark()
queries_sp = Queries_SP(spark=spark, ruta= HADOOP_URI)
queries_pg = Queries_PG(
    spark=spark,
    url=os.getenv("POSTGRESS_URL"),
    user=os.getenv("POSTGRESS_USER"),
    password=os.getenv("POSTGRESS_PASSWORD"),
    driver=os.getenv("POSTGRESS_DRIVER")
)
queries_mg = Queries_MG(
    spark=spark,
    uri=os.getenv("MONGODB_URI"),
    base_datos=os.getenv("MONGO_DATABASE"),
)

# HDFS: consultas genéricas sobre archivos (listar, cargar, agrupar)
queries_hdfs = Queries_HDFS(spark,
    comando=os.getenv("HDFS_CMD", "docker exec namenode hdfs dfs"),
    comando_hadoop=os.getenv("HADOOP_CMD", "docker exec namenode hadoop"),
)

# Instanciacion del dashboard.

filtro_zonas_validas = ["AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "IA", "ID", "IL", # Accidents data set 
"IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", "NJ",
"NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI",
"WV", "WY", 

"Boston - Roxbury", "Bronx - IS52", "McMillan Reservoir", "Essex",                                    # Pollution dataset

"The Esplanade, Portarlington", "Mount Anakie, Anakie", "Sophie Way, Armstrong Creek",                # Australia Clima dataset
"Challis Avenue, Breamlea", "Tower Road, Portarlington", "Jeffrey Street, Indented Head",
"Adella Court, Moolap", "Spectacle Way, Leopold", "Seaspray Court, Indented Head",
"The Esplanade, Indented Head"
]

filtro_origenes_validos = ["Departamento de transporte EE. UU.",    # Accidents data set 
"Victorian Government, Australia",                                  # Australia Clima dataset 
"Open AQ"]                                                          # Pollution dataset

filtro_categorias_validas = ["accidentes",  # Accidents data set 
    "temperature", "humidity",              # Australia Clima dataset 
    "o3", "pm10", "pm25"]                   # Pollution dataset

filtros = Filtros(
    zonas_validas=filtro_zonas_validas,
    origenes_validos=filtro_origenes_validos,
    categorias_validas=filtro_categorias_validas,
    max_ranking=10
)

Dashboard(queries=queries_sp, 
        queries_pg=queries_pg,
        queries_mg=queries_mg,
        queries_hdfs=queries_hdfs,
        filtros=filtros).run()
        