import streamlit as st
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from pathlib import Path
from Dashboard import Dashboard
from queries import Queries_SP, Queries_PG, Queries_HDFS, Queries_MG

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

Dashboard(queries=queries_sp, 
          queries_pg=queries_pg,
          queries_mg=queries_mg,
          queries_hdfs=queries_hdfs).run()