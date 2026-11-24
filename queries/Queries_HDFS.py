import math
import re
import shlex
import subprocess
import uuid
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType


class Queries_HDFS:
    """
    HDFS y Hadoop MapReduce en una sola clase. Todos los métodos que devuelven datos
    devuelven DataFrames de Spark.

    - Comandos de HDFS (`hdfs dfs ...` dentro del contenedor namenode):
        listar, existe, es_carpeta, crear_carpeta, subir
    - Lectura de archivos con Spark: cargar_csv
    - Consultas MapReduce (`hadoop jar ...`): agrupar, contar_palabras, ejecutar_streaming.
      El resultado del job se lee con `hdfs dfs -cat` y se convierte en DataFrame de Spark.

    Tus datos viven en `base_uri` (ej. hdfs://namenode:9000/data/processed/): las rutas
    relativas ("ventas.csv", "ventas_dir") se buscan ahí; las que empiezan con "/" o "hdfs://"
    se usan tal cual.

    No necesitas Hadoop en tu PC: los comandos se ejecutan con `docker exec namenode ...`.
    Con docker exec, las rutas LOCALES (ej. en `subir`) son las del contenedor
    (tu carpeta ./datasets está montada en /datasets).
    Sin YARN en el cluster, Hadoop ejecuta los jobs en modo local (una sola JVM del namenode).
    """

    # Operaciones del reducer (k = clave del grupo)
    OPERACIONES = {"sum": "s[k]", "count": "c[k]", "avg": "s[k]/c[k]", "min": "mn[k]", "max": "mx[k]"}
    SEPARADORES = {",", ";", "|"}
    OPERADORES_FILTRO = {"==", "!=", ">", ">=", "<", "<="}
    _TEXTO_SEGURO = re.compile(r"^[\w .\-]+$")   # sin comillas, $, \ ni otros caracteres especiales

    _ESQUEMA_LISTAR = StructType([
        StructField("tipo", StringType()),
        StructField("permisos", StringType()),
        StructField("replicas", StringType()),
        StructField("propietario", StringType()),
        StructField("grupo", StringType()),
        StructField("tamano_bytes", LongType()),
        StructField("modificado", TimestampType()),
        StructField("ruta", StringType()),
    ])

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
        spark: SparkSession,
        base_uri: str = "hdfs://namenode:9000/data/processed/",
        comando: str = "docker exec namenode hdfs dfs",
        comando_hadoop: str = "docker exec namenode hadoop",
        jar_streaming: str = "/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar",
        jar_ejemplos: str = "/opt/hadoop-3.2.1/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.2.1.jar",
        carpeta_salida: str = "/tmp/dashboard_mr",
        timeout: int = 60,
        timeout_job: int = 900,
    ):
        self.spark = spark
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

    def listar(self, ruta: str = "") -> DataFrame:
        """`hdfs dfs -ls <ruta>` como DataFrame de Spark (sin ruta: la carpeta base)."""
        salida = self._ejecutar("-ls", self._resolver(ruta))
        filas = []
        for linea in salida.splitlines():
            if linea.startswith("Found"):
                continue
            p = linea.split(None, 7)   # la ruta (último campo) puede tener espacios
            if len(p) < 8:
                continue
            filas.append((
                "carpeta" if p[0].startswith("d") else "archivo",
                p[0], p[1], p[2], p[3], int(p[4]),
                datetime.strptime(f"{p[5]} {p[6]}", "%Y-%m-%d %H:%M"),
                p[7],
            ))
        return self.spark.createDataFrame(filas, self._ESQUEMA_LISTAR)

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

    def cargar_csv(self, ruta: str, limite: int = 10000, sep: str = ",",
                   encabezado: bool = True, inferir_tipos: bool = True) -> DataFrame:
        """
        Lee un CSV (archivo, carpeta con varios archivos o patrón) de HDFS con Spark y devuelve
        un DataFrame limitado a `limite` filas. Con inferir_tipos=False todas las columnas
        quedan como texto y Spark solo lee lo mínimo (útil para obtener los nombres de columna).
        """
        return (
            self.spark.read.option("header", encabezado).option("sep", sep)
            .option("inferSchema", inferir_tipos)
            .csv(self._resolver(ruta))
            .limit(int(limite))
        )

    def crear_carpeta(self, ruta: str) -> None:
        """`hdfs dfs -mkdir -p <ruta>`"""
        self._ejecutar("-mkdir", "-p", self._resolver(ruta))

    def subir(self, origen_local: str, destino: str) -> None:
        """`hdfs dfs -put -f <origen_local> <destino>` (origen = ruta vista por el comando)."""
        self._ejecutar("-put", "-f", self._validar_local(origen_local), self._resolver(destino))

    # =====================================================================
    #  Hadoop MapReduce sobre los datos de HDFS
    # =====================================================================
    def _correr_job(self, jar: str, armar_args) -> str:
        """
        Ejecuta `hadoop jar <jar> <args>` con una carpeta de salida única, lee part-*
        y limpia la carpeta temporal. `armar_args(salida)` devuelve los argumentos del job.
        Devuelve el texto de salida del job (líneas `clave<TAB>valor`).
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
            return self._ejecutar("-cat", f"{salida}/part-*")
        finally:
            # Solo se borra la carpeta temporal que creó este job
            if salida.startswith(self.carpeta_salida + "/"):
                try:
                    self._ejecutar("-rm", "-r", "-f", salida)
                except Exception:
                    pass

    def _texto_a_df(self, texto: str, columnas) -> DataFrame:
        """Convierte líneas separadas por tabulador en un DataFrame de Spark (columnas de texto)."""
        n = len(columnas)
        filas = []
        for linea in texto.splitlines():
            if not linea.strip():
                continue
            partes = linea.split("\t", n - 1)
            partes += [None] * (n - len(partes))
            filas.append(tuple(partes))
        esquema = StructType([StructField(c, StringType(), True) for c in columnas])
        return self.spark.createDataFrame(filas, esquema)

    def ejecutar_streaming(self, entrada: str, mapper: str, reducer: str,
                           columnas: tuple = ("clave", "valor")) -> DataFrame:
        """
        Hadoop Streaming: mapper y reducer son comandos que leen stdin y escriben
        líneas `clave<TAB>valor` en stdout. Devuelve la salida como DataFrame de Spark
        (todas las columnas de texto).
        """
        def args(salida):
            return ["-D", "mapreduce.job.reduces=1", "-input", self._resolver(entrada), "-output", salida,
                    "-mapper", mapper, "-reducer", reducer]
        return self._texto_a_df(self._correr_job(self.jar_streaming, args), list(columnas))

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

    def agrupar(self, ruta: str, col_grupo, col_valor, operacion: str = "sum",
                sep: str = ",", encabezado: bool = True, donde: list | None = None) -> DataFrame:
        """
        GROUP BY con MapReduce sobre datos que YA están en HDFS. Devuelve un DataFrame de Spark
        con dos columnas (grupo y valor numérico) ordenado de mayor a menor.

        ruta        archivo, carpeta (todos sus archivos) o patrón, relativos a base_uri (ventas_*.csv)
        col_grupo   nombre de columna (si hay encabezado) o posición 1, 2, 3...
        col_valor   columna numérica a agregar (nombre o posición)
        operacion   sum, count, avg, min o max
        donde       filtros opcionales (AND): [("region", "==", "Norte"), ("ingresos", ">", 100)]

        Ejemplos:
          agrupar("ventas.csv", "region", "ingresos", "sum")
          agrupar("ventas_dir", 2, 4, "avg", encabezado=False, donde=[(2, "==", "Norte")])
        Limitación: CSV simple (sin separadores dentro de campos entre comillas).
        """
        if operacion not in self.OPERACIONES:
            raise ValueError(f"Operación inválida: {operacion}. Usa: {sorted(self.OPERACIONES)}")
        if sep not in self.SEPARADORES:
            raise ValueError(f"Separador inválido: {sep!r}. Usa uno de {sorted(self.SEPARADORES)}")
        ruta = self._resolver(ruta).rstrip("/") or "/"

        # Solo se leen los nombres de columna (con Spark) si hace falta resolverlos
        usa_nombres = encabezado and any(
            isinstance(c, str) for c in [col_grupo, col_valor, *[d[0] for d in (donde or [])]]
        )
        nombres = None
        if usa_nombres:
            nombres = list(self.cargar_csv(ruta, limite=1, sep=sep, inferir_tipos=False).columns)

        g = self._posicion(col_grupo, nombres)
        v = self._posicion(col_valor, nombres)
        cond = self._condicion_awk(donde, nombres)

        accion = f"{{print ${g} \"\\t\" ${v}}}"
        mapper = f"awk -F{sep} '{cond + ' ' if cond else ''}{accion}'"
        reducer = self._REDUCER.replace("__EXPR__", self.OPERACIONES[operacion])

        nombre_g = col_grupo if isinstance(col_grupo, str) else f"col{col_grupo}"
        nombre_v = col_valor if isinstance(col_valor, str) else f"col{col_valor}"
        nombre_val = f"{operacion}_{nombre_v}"

        res = self.ejecutar_streaming(ruta, mapper, reducer, columnas=(nombre_g, nombre_val))
        columna = F.col(f"`{nombre_val}`")
        return res.withColumn(nombre_val, columna.cast("double")).orderBy(columna.desc())

    def contar_palabras(self, ruta: str, top: int = 20) -> DataFrame:
        """Job `wordcount` incluido en Hadoop; sirve también para comprobar que Hadoop funciona."""
        def args(salida):
            return ["wordcount", self._resolver(ruta), salida]
        df = self._texto_a_df(self._correr_job(self.jar_ejemplos, args), ["palabra", "conteo"])
        return df.withColumn("conteo", F.col("conteo").cast("long")).orderBy(F.col("conteo").desc()).limit(int(top))