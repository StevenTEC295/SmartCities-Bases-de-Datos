import json

import pandas as pd
import streamlit as st
from pyspark.sql import DataFrame, SparkSession


class Queries_MG:
    """
    Consultas a MongoDB SOLO con Aggregation Framework.
    Cada consulta es un pipeline (lista de etapas) que se ejecuta dentro de Mongo
    mediante mongo-spark-connector 10.x, vía Spark Connect.
    """

    # El dashboard es de solo lectura: se rechazan las etapas que escriben datos
    ETAPAS_PROHIBIDAS = {"$out", "$merge"}

    def __init__(self, spark: SparkSession, uri: str, base_datos: str):
        self.spark = spark
        self.uri = uri                # ej: mongodb://mongo:27017  (nombre del servicio, puerto interno)
        self.base_datos = base_datos

    # ---------- Núcleo ----------
    @classmethod
    def _validar(cls, pipeline: list) -> None:
        if not isinstance(pipeline, list) or not pipeline:
            raise ValueError("El pipeline debe ser una lista no vacía de etapas ($match, $group, ...)")
        for etapa in pipeline:
            if not isinstance(etapa, dict) or len(etapa) != 1:
                raise ValueError(f"Cada etapa debe ser un dict con un solo operador: {etapa}")
            operador = next(iter(etapa))
            if not operador.startswith("$"):
                raise ValueError(f"Operador de etapa inválido: {operador}")
            if operador in cls.ETAPAS_PROHIBIDAS:
                raise ValueError(f"La etapa {operador} no está permitida (solo lectura)")

    def _agregar(self, coleccion: str, pipeline: list) -> DataFrame:
        self._validar(pipeline)
        df = (
            self.spark.read.format("mongodb")
            .option("connection.uri", self.uri)
            .option("database", self.base_datos)
            .option("collection", coleccion)
            .option("aggregation.pipeline", json.dumps(pipeline))
            .load()
        )
        # _id llega como struct (ObjectId) y no se grafica bien: se descarta si quedó
        return df.drop("_id") if "_id" in df.columns else df

    @st.cache_data(show_spinner="Ejecutando aggregation en MongoDB...")
    def agregar(_self, coleccion: str, pipeline: list) -> pd.DataFrame:
        """Ejecuta un pipeline de agregación y devuelve el resultado como pandas."""
        return _self._agregar(coleccion, pipeline).toPandas()

    # ---------- Consultas de ejemplo (cambia colección y campos por los de tu base) ----------
    def conteo_por_campo(self, coleccion: str, campo: str) -> pd.DataFrame:
        return self.agregar(coleccion, [
            {"$group": {"_id": f"${campo}", "total": {"$sum": 1}}},
            {"$project": {"_id": 0, campo: "$_id", "total": 1}},
            {"$sort": {"total": -1}},
        ])

    def top_n(self, coleccion: str, campo_grupo: str, campo_valor: str, n: int = 10) -> pd.DataFrame:
        return self.agregar(coleccion, [
            {"$group": {"_id": f"${campo_grupo}", "total": {"$sum": f"${campo_valor}"}}},
            {"$sort": {"total": -1}},
            {"$limit": int(n)},
            {"$project": {"_id": 0, campo_grupo: "$_id", "total": 1}},
        ])

    def serie_diaria(self, coleccion: str, campo_fecha: str, campo_valor: str) -> pd.DataFrame:
        # Requiere que campo_fecha sea tipo Date en Mongo (si es texto, usa {"$toDate": ...})
        return self.agregar(coleccion, [
            {"$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": f"${campo_fecha}"}},
                "promedio": {"$avg": f"${campo_valor}"},
                "maximo": {"$max": f"${campo_valor}"},
            }},
            {"$project": {"_id": 0, "fecha": "$_id", "promedio": 1, "maximo": 1}},
            {"$sort": {"fecha": 1}},
        ])