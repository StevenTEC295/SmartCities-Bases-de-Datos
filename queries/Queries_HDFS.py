import math
import re
import shlex
import subprocess
import uuid
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

from .Queries_SP import Queries_SP   # reutiliza el mapeo estado -> zona y las estaciones de contaminación


class Queries_HDFS:
    """
    HDFS y Hadoop MapReduce en una sola clase. Todos los métodos que devuelven datos
    devuelven DataFrames de Spark.
    """

    # Operaciones del reducer (k = clave del grupo)
    OPERACIONES = {"sum": "s[k]", "count": "c[k]", "avg": "s[k]/c[k]", "min": "mn[k]", "max": "mx[k]"}
    SEPARADORES = {",", ";", "|"}
    OPERADORES_FILTRO = {"==", "!=", ">", ">=", "<", "<="}
    _TEXTO_SEGURO = re.compile(r"^[\w .\-]+$")   

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
        ruta_texto: str | None = None,
    ):
        self.spark = spark
        self.base_uri = base_uri.rstrip("/")    
        self.ruta_texto = (ruta_texto or self.base_uri.rsplit("/", 1)[0] + "/mapreduce").rstrip("/")
        self.comando = shlex.split(comando)
        self.comando_hadoop = shlex.split(comando_hadoop)
        self.jar_streaming = jar_streaming
        self.jar_ejemplos = jar_ejemplos
        self.carpeta_salida = carpeta_salida.rstrip("/")
        self.timeout = timeout
        self.timeout_job = timeout_job

 
    def _resolver(self, ruta: str | None = "") -> str:
      
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


    #  Hadoop MapReduce sobre los datos de HDFS
   
    def _correr_job(self, jar: str, armar_args) -> str:
       
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
     
        if operacion not in self.OPERACIONES:
            raise ValueError(f"Operación inválida: {operacion}. Usa: {sorted(self.OPERACIONES)}")
        if sep not in self.SEPARADORES:
            raise ValueError(f"Separador inválido: {sep!r}. Usa uno de {sorted(self.SEPARADORES)}")
        ruta = self._resolver(ruta).rstrip("/") or "/"

        # Solo se leen los nombres de columna
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
       
        def args(salida):
            return ["wordcount", self._resolver(ruta), salida]
        df = self._texto_a_df(self._correr_job(self.jar_ejemplos, args), ["palabra", "conteo"])
        return df.withColumn("conteo", F.col("conteo").cast("long")).orderBy(F.col("conteo").desc()).limit(int(top))

   
    #  Operaciones de HDFS para la demostración

    def _comando_hdfs(self, *args: str) -> list[str]:
        """Arma `hdfs <args>` """
        base = self.comando[:-1] if self.comando and self.comando[-1] == "dfs" else self.comando
        return [*base, *args]

    def leer_archivo(self, ruta: str, lineas: int = 10) -> DataFrame:
        """`hdfs dfs -cat <ruta>`"""
        proceso = subprocess.Popen(
            [*self.comando, "-cat", self._resolver(ruta)],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, errors="replace",
        )
        filas = []
        try:
            for linea in proceso.stdout:
                filas.append((linea.rstrip("\n"),))
                if len(filas) >= int(lineas):
                    break
        finally:
            proceso.kill()
        return self.spark.createDataFrame(filas, StructType([StructField("linea", StringType())]))

    def descargar(self, ruta_hdfs: str, destino_local: str) -> None:
        """`hdfs dfs -get <ruta_hdfs> <destino_local>`."""
        self._ejecutar("-get", "-f", self._resolver(ruta_hdfs), self._validar_local(destino_local))

    def eliminar(self, ruta: str, recursivo: bool = False) -> None:
        """`hdfs dfs -rm [-r] <ruta>`"""
        ruta = self._resolver(ruta)
        if ruta.rstrip("/").endswith("/data") or ruta.rstrip("/") in ("", "/", "hdfs://namenode:9000"):
            raise ValueError("No se permite borrar la carpeta raíz de los datos")
        self._ejecutar("-rm", *(["-r"] if recursivo else []), ruta)

    def bloques(self, ruta: str) -> DataFrame:
        """`hdfs fsck <ruta> -files -blocks -locations`: un renglón por bloque, con el/los datanodes que lo guardan."""
        r = subprocess.run(
            self._comando_hdfs("fsck", self._resolver(ruta), "-files", "-blocks", "-locations"),
            capture_output=True, text=True, timeout=self.timeout_job,
        )
        patron = re.compile(r"^(\d+)\. \S+:(blk_\d+)_\d+ len=(\d+) Live_repl=(\d+)\s+\[(.*)\]")
        filas = []
        for linea in r.stdout.splitlines():
            m = patron.match(linea.strip())
            if m:
                nodos = ", ".join(sorted(set(re.findall(r"DatanodeInfoWithStorage\[([\d.]+:\d+)", m.group(5)))))
                filas.append((int(m.group(1)), m.group(2), int(m.group(3)), int(m.group(4)), nodos))
        esquema = "orden INT, bloque STRING, tamano_bytes LONG, replicas_vivas INT, datanodes STRING"
        return self.spark.createDataFrame(filas, esquema)

    def resumen_cluster(self) -> DataFrame:
        """Estado del clúster datanodes vivos, bloques, replicación."""
        r = subprocess.run(self._comando_hdfs("dfsadmin", "-report"), capture_output=True, text=True, timeout=self.timeout)
        buscar = {
            "Datanodes vivos": r"Live datanodes \((\d+)\)",
            "Capacidad configurada": r"Configured Capacity: \d+ \(([^)]+)\)",
            "Espacio usado": r"DFS Used: \d+ \(([^)]+)\)",
            "Bloques sub-replicados": r"Under replicated blocks: (\d+)",
            "Bloques corruptos": r"Blocks with corrupt replicas: (\d+)",
            "Bloques perdidos": r"Missing blocks: (\d+)",
        }
        filas = []
        for nombre, patron in buscar.items():
            m = re.search(patron, r.stdout)
            filas.append((nombre, m.group(1) if m else None))
        return self.spark.createDataFrame(filas, "metrica STRING, valor STRING")

    #  Consultas obligatorias de Hadoop 
  
  
    TEXTO = {
        "contaminacion": "contaminacion",   # 1 sensor_id, 2 fecha_utc, 3 valor, 4 zona
        "clima": "clima",                   # 1 dev_id, 2 fecha_utc, 3 temperatura (°C)
        "accidentes": "accidentes",         # 1 estado, 2 zona, 3 fecha_utc, 4 severidad
    }
    _FECHA = re.compile(r"^\d{4}-\d{2}-\d{2}$")

    _REDUCER_RESUMEN = (
        r"""awk 'BEGIN{FS="\t"} """
        r"""$2 ~ /^-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?$/ { """
        r"""s[$1]+=$2; c[$1]++; """
        r"""if (!($1 in mn) || $2+0 < mn[$1]) mn[$1]=$2+0; """
        r"""if (!($1 in mx) || $2+0 > mx[$1]) mx[$1]=$2+0 } """
        r"""END { for (k in s) printf "%s\t%d\t%.4f\t%.4f\t%.4f\n", k, c[k], s[k]/c[k], mn[k], mx[k] }'"""
    )

    def preparar_texto(self) -> DataFrame:
        """
        Genera el texto limpio que lee MapReduce a partir de los Parquet de la capa processed.
        Devuelve un DataFrame con las filas escritas por dataset.
        """
        sp = Queries_SP(self.spark, self.base_uri)
        zonas = sp._estado_zona().select("estado", "zona")
        sin_zona = F.coalesce(F.col("zona"), F.lit("Sin zona"))

        sensor_zona = (
            sp._leer("contaminacion/sensores").select("sensor_id", "location_id")
            .join(sp._ubicaciones_cont().select("location_id", "estado"), "location_id", "left")
            .join(zonas, "estado", "left")
            .select("sensor_id", sin_zona.alias("zona"))
        )
        contaminacion = (
            sp._leer("contaminacion/mediciones").join(sensor_zona, "sensor_id", "left")
            .select(
                F.col("sensor_id").cast("string"),
                F.date_format("datetime_utc", "yyyy-MM-dd HH:mm:ss"),
                F.col("value").cast("double"),
                F.coalesce(F.col("zona"), F.lit("Sin zona")),
            )
        )
        clima = (
            sp._leer("clima/mediciones")
            .select(
                F.col("dev_id").cast("string"),
                F.date_format("time", "yyyy-MM-dd HH:mm:ss"),
                F.col("temperature").cast("double"),
            )
        )
        accidentes = (
            self.spark.read.parquet(sp.ruta_accidentes)
            .join(zonas, F.col("state") == zonas["estado"], "left")
            .select(
                F.col("state"),
                sin_zona,
                F.date_format("start_time_utc", "yyyy-MM-dd HH:mm:ss"),
                F.col("severity").cast("int"),
            )
        )

        conteos = []
        for nombre, df in (("contaminacion", contaminacion), ("clima", clima), ("accidentes", accidentes)):
            destino = f"{self.ruta_texto}/{self.TEXTO[nombre]}"
            df.write.mode("overwrite").option("header", False).option("sep", ",").csv(destino)
            # Una sola réplica: el clúster tiene un único datanode (evita bloques sub-replicados)
            try:
                self._ejecutar("-setrep", "-R", "1", destino)
            except RuntimeError:
                pass
            conteos.append((nombre, self.spark.read.csv(destino).count(), destino))
        return self.spark.createDataFrame(conteos, "dataset STRING, filas LONG, ruta STRING")

    def texto_listo(self) -> bool:
        """True si ya existe el texto limpio de los tres datasets."""
        return all(self.existe(f"{self.ruta_texto}/{n}") for n in self.TEXTO.values())

    def _resumen(self, dataset: str, col_clave: int, col_valor: int, col_fecha: int | None = None,
                 fecha_ini: str | None = None, fecha_fin: str | None = None,
                 etiqueta_total: str | None = None) -> DataFrame:
       
        condiciones = []
        for fecha, operador, hora in ((fecha_ini, ">=", "00:00:00"), (fecha_fin, "<=", "23:59:59")):
            if fecha:
                if not self._FECHA.match(fecha):
                    raise ValueError(f"Fecha inválida: {fecha!r} (use AAAA-MM-DD)")
                condiciones.append(f'${col_fecha} {operador} "{fecha} {hora}"')
        if etiqueta_total is not None and not self._TEXTO_SEGURO.match(etiqueta_total):
            raise ValueError(f"Etiqueta inválida: {etiqueta_total!r}")

        accion = f'print ${col_clave} "\\t" ${col_valor}'
        if etiqueta_total:
            accion += f'; print "{etiqueta_total}" "\\t" ${col_valor}'
        mapper = f"awk -F, '{' && '.join(condiciones)} {{{accion}}}'"

        df = self.ejecutar_streaming(
            f"{self.ruta_texto}/{self.TEXTO[dataset]}", mapper, self._REDUCER_RESUMEN,
            columnas=("clave", "mediciones", "promedio", "minimo", "maximo"),
        )
        return (
            df.withColumn("mediciones", F.col("mediciones").cast("long"))
              .withColumn("promedio", F.col("promedio").cast("double"))
              .withColumn("minimo", F.col("minimo").cast("double"))
              .withColumn("maximo", F.col("maximo").cast("double"))
        )

    def _nombres_sensores(self) -> DataFrame:
        """fuente, sensor_id (texto), nombre y variable de cada sensor """
        sp = Queries_SP(self.spark, self.base_uri)
        cont = (
            sp._leer("contaminacion/sensores")
            .join(sp._leer("contaminacion/locations"), "location_id", "left")
            .join(sp._leer("contaminacion/parametros"), "parameter_id", "left")
            .select(
                F.lit("contaminacion").alias("fuente"),
                F.col("sensor_id").cast("string").alias("sensor_id"),
                F.col("location_name").alias("nombre"),
                F.col("parameter").alias("variable"),
            )
        )
        clima = sp._leer("clima/sensores").select(
            F.lit("clima").alias("fuente"),
            F.col("dev_id").cast("string").alias("sensor_id"),
            F.col("name").alias("nombre"),
            F.lit("temperature").alias("variable"),
        )
        return cont.unionByName(clima)

    def _por_sensor(self, fuente: str | None, fecha_ini: str | None, fecha_fin: str | None) -> DataFrame:
        """Resumen por sensor con nombre y variable. Base de las preguntas 1, 2 y 5."""
        partes = []
        for nombre in ("contaminacion", "clima"):
            if fuente in (None, nombre):
                partes.append(
                    self._resumen(nombre, 1, 3, 2, fecha_ini, fecha_fin)
                    .withColumn("fuente", F.lit(nombre))
                    .withColumnRenamed("clave", "sensor_id")
                )
        if not partes:
            raise ValueError("fuente debe ser 'contaminacion', 'clima' o None")
        union = partes[0] if len(partes) == 1 else partes[0].unionByName(partes[1])
        return union.join(self._nombres_sensores(), ["fuente", "sensor_id"], "left")

    # 1. ¿Cuántas mediciones existen por sensor?
    def mediciones_por_sensor(self, fuente: str | None = None, fecha_ini: str | None = None,
                              fecha_fin: str | None = None) -> DataFrame:
        return (
            self._por_sensor(fuente, fecha_ini, fecha_fin)
            .select("fuente", "sensor_id", "nombre", "variable", "mediciones")
            .orderBy(F.desc("mediciones"), F.asc("sensor_id"))
        )

    # 2. ¿Cuál sensor produjo mayor cantidad de datos?
    def sensor_con_mas_datos(self, n: int = 5, fuente: str | None = None, fecha_ini: str | None = None,
                             fecha_fin: str | None = None) -> DataFrame:
        todos = self.mediciones_por_sensor(fuente, fecha_ini, fecha_fin)
        total = Window.partitionBy()
        return (
            todos.withColumn("porcentaje_del_total", F.round(F.col("mediciones") * 100.0 / F.sum("mediciones").over(total), 2))
            .limit(int(n))
        )

    # 3. ¿Cuál es la temperatura promedio histórica? (clima, grados Celsius)
    def temperatura_promedio_historica(self, fecha_ini: str | None = None, fecha_fin: str | None = None) -> DataFrame:
        """Promedio de la temperatura en °C: primero el global y luego por dispositivo."""
        df = self._resumen("clima", 1, 3, 2, fecha_ini, fecha_fin, etiqueta_total="TODOS")
        nombres = self._nombres_sensores().filter(F.col("fuente") == "clima").select(
            F.col("sensor_id").alias("clave"), "nombre"
        )
        return (
            df.join(nombres, "clave", "left")
            .select(
                F.col("clave").alias("sensor_id"),
                F.coalesce(F.col("nombre"), F.when(F.col("clave") == "TODOS", F.lit("Todos los sensores"))).alias("nombre"),
                "mediciones",
                F.round("promedio", 2).alias("promedio_c"),
                F.round("minimo", 2).alias("minimo_c"),
                F.round("maximo", 2).alias("maximo_c"),
            )
            .orderBy((F.col("sensor_id") != "TODOS"), F.desc("promedio_c"))
        )

    # 4. ¿Cuál es la cantidad de eventos por zona?
    def eventos_por_zona(self, fecha_ini: str | None = None, fecha_fin: str | None = None) -> DataFrame:
        """Eventos = accidentes (zona del estado) y mediciones de contaminación (zona de la estación)."""
        acc = (
            self._resumen("accidentes", 2, 4, 3, fecha_ini, fecha_fin)
            .select(F.lit("accidentes").alias("dataset"), F.col("clave").alias("zona"), F.col("mediciones").alias("eventos"))
        )
        cont = (
            self._resumen("contaminacion", 4, 3, 2, fecha_ini, fecha_fin)
            .select(F.lit("contaminacion").alias("dataset"), F.col("clave").alias("zona"), F.col("mediciones").alias("eventos"))
        )
        return acc.unionByName(cont).orderBy(F.asc("dataset"), F.desc("eventos"))

    # 5. ¿Cuál es el comportamiento de los sensores durante un período determinado?
    def comportamiento_sensores(self, fecha_ini: str | None = None, fecha_fin: str | None = None,
                                fuente: str | None = None) -> DataFrame:
        """Por sensor en el periodo: cantidad de mediciones, promedio, mínimo y máximo."""
        return (
            self._por_sensor(fuente, fecha_ini, fecha_fin)
            .select(
                "fuente", "sensor_id", "nombre", "variable", "mediciones",
                F.round("promedio", 2).alias("promedio"),
                F.round("minimo", 2).alias("minimo"),
                F.round("maximo", 2).alias("maximo"),
            )
            .orderBy(F.asc("fuente"), F.desc("mediciones"))
        )

    # Registro de preguntas: id -> (descripción, método)
    CONSULTAS = {
        1: ("Mediciones por sensor", "mediciones_por_sensor"),
        2: ("Sensor que produjo mayor cantidad de datos", "sensor_con_mas_datos"),
        3: ("Temperatura promedio histórica (°C)", "temperatura_promedio_historica"),
        4: ("Cantidad de eventos por zona", "eventos_por_zona"),
        5: ("Comportamiento de los sensores en un periodo", "comportamiento_sensores"),
    }

    def ejecutar(self, numero: int, **parametros) -> DataFrame:
        """Ejecuta una pregunta de Hadoop por su número (ver CONSULTAS)."""
        if numero not in self.CONSULTAS:
            raise ValueError(f"Consulta {numero} no disponible. Disponibles: {sorted(self.CONSULTAS)}")
        return getattr(self, self.CONSULTAS[numero][1])(**parametros)
