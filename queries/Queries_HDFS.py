import math
import re
import shlex
import subprocess
import uuid
from io import StringIO

import pandas as pd
import streamlit as st


class Queries_HDFS:
    """
    HDFS y Hadoop MapReduce en una sola clase, usando los comandos del contenedor namenode.

    - Comandos de HDFS:   `hdfs dfs ...`  -> listar, existe, es_carpeta, cargar_csv, crear_carpeta, subir
    - Consultas MapReduce: `hadoop jar ...` -> agrupar, contar_palabras, ejecutar_streaming

    Tus datos viven en `base_uri` (ej. hdfs://namenode:9000/data/processed/): las rutas
    relativas ("ventas.csv", "ventas_dir") se buscan ahí; las que empiezan con "/" o "hdfs://"
    se usan tal cual.

    No necesitas Hadoop en tu PC: todo se ejecuta con `docker exec namenode ...`.
    Con docker exec, las rutas LOCALES (ej. en `subir`) son las del contenedor
    (tu carpeta ./datasets está montada en /datasets).
    Sin YARN en el cluster, Hadoop ejecuta los jobs en modo local (una sola JVM del namenode).
    """

    # Operaciones del reducer (k = clave del grupo)
    OPERACIONES = {"sum": "s[k]", "count": "c[k]", "avg": "s[k]/c[k]", "min": "mn[k]", "max": "mx[k]"}
    SEPARADORES = {",", ";", "|"}
    OPERADORES_FILTRO = {"==", "!=", ">", ">=", "<", "<="}
    _TEXTO_SEGURO = re.compile(r"^[\w .\-]+$")   # sin comillas, $, \ ni otros caracteres especiales

    # Reducer en awk: ignora filas cuyo valor no es numérico (p. ej. el encabezado del CSV)
    _REDUCER = (
        "awk 'BEGIN{FS=\"\\t\"} "
        "$2 ~ /^-?[0-9]+(\\.[0-9]+)?([eE][-+]?[0-9]+)?$/ { "
        "s[$1]+=$2; c[$1]++; "
        "if (!($1 in mn) || $2+0 < mn[$1]) mn[$1]=$2+0; "
        "if (!($1 in mx) || $2+0 > mx[$1]) mx[$1]=$2+0 } "
        "END { for (k in s) printf \"%s\\t%.4f\\n\", k, __EXPR__ }'"
    )

    def __init__(
        self,
        base_uri: str = "hdfs://namenode:9000/data/processed/",
        comando: str = "docker exec namenode hdfs dfs",
        comando_hadoop: str = "docker exec namenode hadoop",
        jar_streaming: str = "/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar",
        jar_ejemplos: str = "/opt/hadoop-3.2.1/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.2.1.jar",
        carpeta_salida: str = "/tmp/dashboard_mr",
        timeout: int = 60,
        timeout_job: int = 900,
    ):
        self.base_uri = base_uri.rstrip("/")     # carpeta de tus datos en HDFS
        self.comando = shlex.split(comando)
        self.comando_hadoop = shlex.split(comando_hadoop)
        self.jar_streaming = jar_streaming
        self.jar_ejemplos = jar_ejemplos
        self.carpeta_salida = carpeta_salida.rstrip("/")
        self.timeout = timeout
        self.timeout_job = timeout_job

    # =====================================================================
    #  Comandos de HDFS
    # =====================================================================
    def _resolver(self, ruta: str | None = "") -> str:
        """
        Convierte una ruta en la ruta completa de HDFS:
          ""  o  "ventas.csv"        -> relativa a base_uri  (hdfs://namenode:9000/data/processed/...)
          "/otra/carpeta/x.csv"      -> ruta absoluta de HDFS, tal cual
          "hdfs://host:9000/x.csv"   -> URI completa, tal cual
        """
        ruta = (ruta or "").strip()
        if ruta.startswith("-"):   # evita que una ruta se interprete como opción del comando (ej. "-rm")
            raise ValueError(f"Ruta inválida: {ruta!r}")
        if "://" in ruta or ruta.startswith("/"):
            return ruta
        return f"{self.base_uri}/{ruta}" if ruta else self.base_uri

    @staticmethod
    def _validar_local(ruta: str) -> str:
        if not ruta or ruta.startswith("-"):
            raise ValueError(f"Ruta inválida: {ruta!r}")
        return ruta

    def _ejecutar(self, *args: str) -> str:
        """Ejecuta `hdfs dfs <args>` (sin shell) y devuelve la salida; lanza error si falla."""
        r = subprocess.run(
            [*self.comando, *args], capture_output=True, text=True, timeout=self.timeout
        )
        if r.returncode != 0:
            raise RuntimeError(f"hdfs dfs {' '.join(args)} falló: {r.stderr.strip()}")
        return r.stdout

    @st.cache_data(ttl=30, show_spinner="hdfs dfs -ls ...")
    def listar(_self, ruta: str = "") -> pd.DataFrame:
        """`hdfs dfs -ls <ruta>` como DataFrame (sin ruta: la carpeta base)."""
        salida = _self._ejecutar("-ls", _self._resolver(ruta))
        filas = []
        for linea in salida.splitlines():
            if linea.startswith("Found"):
                continue
            p = linea.split(None, 7)   # la ruta (último campo) puede tener espacios
            if len(p) < 8:
                continue
            filas.append(
                {
                    "tipo": "carpeta" if p[0].startswith("d") else "archivo",
                    "permisos": p[0],
                    "replicas": p[1],
                    "propietario": p[2],
                    "grupo": p[3],
                    "tamano_bytes": int(p[4]),
                    "modificado": pd.to_datetime(f"{p[5]} {p[6]}"),
                    "ruta": p[7],
                }
            )
        return pd.DataFrame(
            filas,
            columns=["tipo", "permisos", "replicas", "propietario", "grupo",
                     "tamano_bytes", "modificado", "ruta"],
        )

    def existe(self, ruta: str) -> bool:
        """`hdfs dfs -test -e <ruta>`"""
        r = subprocess.run(
            [*self.comando, "-test", "-e", self._resolver(ruta)],
            capture_output=True, text=True, timeout=self.timeout,
        )
        return r.returncode == 0

    def es_carpeta(self, ruta: str) -> bool:
        """`hdfs dfs -test -d <ruta>`"""
        r = subprocess.run(
            [*self.comando, "-test", "-d", self._resolver(ruta)],
            capture_output=True, text=True, timeout=self.timeout,
        )
        return r.returncode == 0

    @st.cache_data(ttl=30, show_spinner="hdfs dfs -cat ...")
    def cargar_csv(_self, ruta: str, limite: int = 10000, **opciones_pandas) -> pd.DataFrame:
        """
        `hdfs dfs -cat <ruta>` leyendo solo las primeras `limite` filas (más el encabezado):
        corta el comando en cuanto tiene suficientes líneas, sin traer el archivo completo.
        """
        proc = subprocess.Popen(
            [*_self.comando, "-cat", _self._resolver(ruta)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        lineas = []
        try:
            for linea in proc.stdout:
                lineas.append(linea)
                if len(lineas) > int(limite):   # encabezado + limite filas
                    break
        finally:
            proc.terminate()
            proc.wait(timeout=_self.timeout)
        if not lineas:
            raise RuntimeError(f"hdfs dfs -cat {ruta} no devolvió datos: {proc.stderr.read().strip()}")
        return pd.read_csv(StringIO("".join(lineas)), **opciones_pandas)

    def crear_carpeta(self, ruta: str) -> None:
        """`hdfs dfs -mkdir -p <ruta>`"""
        self._ejecutar("-mkdir", "-p", self._resolver(ruta))
        self.listar.clear()

    def subir(self, origen_local: str, destino: str) -> None:
        """`hdfs dfs -put -f <origen_local> <destino>` (origen = ruta vista por el comando)."""
        self._ejecutar("-put", "-f", self._validar_local(origen_local), self._resolver(destino))
        self.listar.clear()

    # =====================================================================
    #  Hadoop MapReduce sobre los datos de HDFS
    # =====================================================================
    def _correr_job(self, jar: str, armar_args, columnas: list) -> pd.DataFrame:
        """
        Ejecuta `hadoop jar <jar> <args>` con una carpeta de salida única, lee part-*
        y limpia la carpeta temporal. `armar_args(salida)` devuelve los argumentos del job.
        """
        salida = f"{self.carpeta_salida}/{uuid.uuid4().hex}"
        try:
            r = subprocess.run(
                [*self.comando_hadoop, "jar", jar, *armar_args(salida)],
                capture_output=True, text=True, timeout=self.timeout_job,
            )
            if r.returncode != 0:
                cola = "\n".join(r.stderr.strip().splitlines()[-15:])
                raise RuntimeError(f"El job de Hadoop falló:\n{cola}")
            texto = self._ejecutar("-cat", f"{salida}/part-*")
        finally:
            # Solo se borra la carpeta temporal que creó este job
            if salida.startswith(self.carpeta_salida + "/"):
                try:
                    self._ejecutar("-rm", "-r", "-f", salida)
                except Exception:
                    pass
        if not texto.strip():
            return pd.DataFrame(columns=columnas)
        return pd.read_csv(StringIO(texto), sep="\t", header=None, names=columnas)

    @st.cache_data(ttl=300, show_spinner="Ejecutando MapReduce (Hadoop)...")
    def ejecutar_streaming(_self, entrada: str, mapper: str, reducer: str,
                           columnas: tuple = ("clave", "valor")) -> pd.DataFrame:
        """
        Hadoop Streaming: mapper y reducer son comandos que leen stdin y escriben
        líneas `clave<TAB>valor` en stdout. Devuelve la salida como DataFrame.
        """
        def args(salida):
            return ["-D", "mapreduce.job.reduces=1", "-input", _self._resolver(entrada), "-output", salida,
                    "-mapper", mapper, "-reducer", reducer]
        return _self._correr_job(_self.jar_streaming, args, list(columnas))

    @staticmethod
    def _posicion(columna, nombres: list | None) -> int:
        """Convierte nombre o posición (1, 2, 3...) a posición numérica de columna."""
        if isinstance(columna, bool):
            raise ValueError(f"Columna inválida: {columna!r}")
        if isinstance(columna, int):
            if columna < 1:
                raise ValueError("Las posiciones de columna empiezan en 1")
            return columna
        if nombres is None:
            raise ValueError("Sin encabezado usa posiciones numéricas (1, 2, 3...)")
        if columna not in nombres:
            raise ValueError(f"La columna {columna!r} no existe. Columnas: {nombres}")
        return nombres.index(columna) + 1

    def _condicion_awk(self, donde, nombres) -> str:
        """Arma el filtro WHERE para el mapper; todos los valores se validan antes."""
        partes = []
        for col, op, valor in donde or []:
            if op not in self.OPERADORES_FILTRO:
                raise ValueError(f"Operador inválido: {op!r}. Usa: {sorted(self.OPERADORES_FILTRO)}")
            pos = self._posicion(col, nombres)
            if isinstance(valor, bool) or not isinstance(valor, (int, float, str)):
                raise ValueError(f"Valor inválido en el filtro: {valor!r}")
            if isinstance(valor, str):
                if not self._TEXTO_SEGURO.match(valor):
                    raise ValueError(f"Texto no permitido en el filtro: {valor!r}")
                if op not in {"==", "!="}:
                    raise ValueError("Con texto solo se permite == o !=")
                partes.append(f'${pos} {op} "{valor}"')
            else:
                if not math.isfinite(valor):
                    raise ValueError(f"Valor numérico inválido: {valor!r}")
                partes.append(f"${pos}+0 {op} {valor}")
        return " && ".join(partes)

    @st.cache_data(ttl=300, show_spinner="Ejecutando MapReduce (Hadoop)...")
    def agrupar(_self, ruta: str, col_grupo, col_valor, operacion: str = "sum",
                sep: str = ",", encabezado: bool = True, donde: list | None = None) -> pd.DataFrame:
        """
        GROUP BY con MapReduce sobre datos que YA están en HDFS.

        ruta        archivo, carpeta (todos sus archivos) o patrón, relativos a base_uri (ventas_*.csv)
        col_grupo   nombre de columna (si hay encabezado) o posición 1, 2, 3...
        col_valor   columna numérica a agregar (nombre o posición)
        operacion   sum | count | avg | min | max
        donde       filtros opcionales (AND): [("region", "==", "Norte"), ("ingresos", ">", 100)]

        Ejemplos:
          agrupar("ventas.csv", "region", "ingresos", "sum")
          agrupar("ventas_dir", 2, 4, "avg", encabezado=False, donde=[(2, "==", "Norte")])
        Limitación: CSV simple (sin separadores dentro de campos entre comillas).
        """
        if operacion not in _self.OPERACIONES:
            raise ValueError(f"Operación inválida: {operacion}. Usa: {sorted(_self.OPERACIONES)}")
        if sep not in _self.SEPARADORES:
            raise ValueError(f"Separador inválido: {sep!r}. Usa uno de {sorted(_self.SEPARADORES)}")
        ruta = _self._resolver(ruta).rstrip("/") or "/"

        # Solo se lee el encabezado si hace falta resolver nombres de columna
        usa_nombres = encabezado and any(
            isinstance(c, str) for c in [col_grupo, col_valor, *[d[0] for d in (donde or [])]]
        )
        nombres = None
        if usa_nombres:
            lectura = f"{ruta}/*" if _self.es_carpeta(ruta) else ruta   # carpeta -> sus archivos
            nombres = list(_self.cargar_csv(lectura, limite=1, sep=sep).columns)

        g = _self._posicion(col_grupo, nombres)
        v = _self._posicion(col_valor, nombres)
        cond = _self._condicion_awk(donde, nombres)

        accion = f"{{print ${g} \"\\t\" ${v}}}"
        mapper = f"awk -F{sep} '{cond + ' ' if cond else ''}{accion}'"
        reducer = _self._REDUCER.replace("__EXPR__", _self.OPERACIONES[operacion])

        nombre_g = col_grupo if isinstance(col_grupo, str) else f"col{col_grupo}"
        nombre_v = col_valor if isinstance(col_valor, str) else f"col{col_valor}"
        res = _self.ejecutar_streaming(ruta, mapper, reducer, columnas=(nombre_g, f"{operacion}_{nombre_v}"))
        return res.sort_values(res.columns[1], ascending=False, ignore_index=True)

    @st.cache_data(ttl=300, show_spinner="Ejecutando MapReduce (Hadoop)...")
    def contar_palabras(_self, ruta: str, top: int = 20) -> pd.DataFrame:
        """Job `wordcount` incluido en Hadoop; sirve también para comprobar que Hadoop funciona."""
        def args(salida):
            return ["wordcount", _self._resolver(ruta), salida]
        df = _self._correr_job(_self.jar_ejemplos, args, ["palabra", "conteo"])
        return df.sort_values("conteo", ascending=False, ignore_index=True).head(int(top))