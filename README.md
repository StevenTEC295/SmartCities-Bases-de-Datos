# Hadoop (HDFS) + Spark con Docker

Entorno local de un solo nodo para almacenar datasets en **HDFS** y consultarlos con **Apache Spark** desde Python, usando **Spark Connect**. Todo el servidor corre en contenedores Linux, así que funciona igual en Windows, macOS y Linux y no requiere instalar Hadoop ni Java en tu máquina.

> Pensado para desarrollo y aprendizaje. Es un clúster de un solo nodo, no un entorno de producción.

## Arquitectura

```
Tu código Python (notebook, Streamlit, Dash...)
        │  sc://localhost:15002  (Spark Connect)
        ▼
┌─────────────── Docker ────────────────┐
│  spark (servidor Spark Connect)       │
│      │  hdfs://namenode:9000          │
│      ▼                                │
│  namenode  ◄──►  datanode  (HDFS)     │
└───────────────────────────────────────┘
        ▲
        │  carpeta ./datasets montada en /datasets
   CSV originales en tu PC
```

| Servicio | Función | Puerto en tu PC |
|---|---|---|
| `namenode` | Metadatos de HDFS e interfaz web | `9870` (web), `9000` (HDFS) |
| `datanode` | Almacena los bloques de datos | `9864`, `9866` |
| `spark` | Servidor Spark Connect | `15002` (conexión), `4040` (web) |

## Requisitos

- **Docker Desktop** (Windows/macOS) o Docker Engine con el plugin Compose (Linux).
  - En Windows, con el backend **WSL2** activado y la virtualización habilitada en la BIOS.
- **Python 3.10 o superior** en tu máquina, para el cliente.
- **Memoria:** al menos 16 GB de RAM recomendados. El servidor Spark usa 4 GB y HDFS un par de GB más.
- **Espacio en disco:** unos 5 GB para las imágenes, más el tamaño de tus datos.

Verificá que Docker funcione:

```bash
docker version
docker compose version
```

`docker version` debe mostrar las secciones **Client** y **Server**. Si solo aparece Client con un error, Docker Desktop no está corriendo.

## Estructura del proyecto

```
mi-proyecto/
├── docker-compose.yml
├── README.md
└── datasets/              <- tus archivos CSV (se monta en el contenedor)
    └── mi_dataset.csv
```

## Paso 1. Preparar la carpeta

1. Copiá `docker-compose.yml` en una carpeta nueva.
2. Creá una subcarpeta `datasets` y poné adentro tus archivos.

Para usar otra carpeta de tu PC, cambiá esta línea del servicio `namenode`, con barras normales (`/`) aunque estés en Windows:

```yaml
      - "D:/MisDatos/proyecto:/datasets"
```

## Paso 2. Levantar los contenedores

Desde la carpeta del proyecto:

```bash
docker compose up -d
```

La primera vez descarga las imágenes (varios GB) y puede tardar unos minutos. Después verificá:

```bash
docker compose ps
```

Tienen que aparecer `namenode`, `datanode` y `spark` en estado **running**. Abrí http://localhost:9870 y comprobá en la sección **Datanodes** que haya **1 nodo activo**. Si no aparece, esperá un minuto y recargá.

## Paso 3. Subir los datos a HDFS

Comprobá que el contenedor ve tus archivos:

```bash
docker exec namenode ls -lh /datasets
```

Creá una carpeta por dataset y subí el archivo (reemplazá los nombres por los tuyos):

```bash
docker exec namenode hdfs dfs -mkdir -p /data/raw/mi_dataset
docker exec namenode hdfs dfs -put -f /datasets/mi_dataset.csv /data/raw/mi_dataset/
docker exec namenode hdfs dfs -ls -h /data/raw/mi_dataset
```

Notas:

- La subida no muestra progreso. Esperá a que vuelva el prompt. Un archivo de 3 GB puede tardar varios minutos.
- Los mensajes `SASL encryption trust check` son informativos, no errores.
- Evitá espacios y tildes en los nombres de archivos y carpetas.
- Si interrumpís la subida queda un archivo `._COPYING_` incompleto. Repetí el comando con `-f`.

También podés explorar HDFS desde http://localhost:9870 → *Utilities → Browse the file system*.

## Paso 4. Conectarte desde Python

Creá un entorno virtual e instalá el cliente. La versión de `pyspark` **debe coincidir** con la de la imagen del servidor (`apache/spark:4.0.0`):

```bash
python -m venv venv
# Windows:  venv\Scripts\activate
# Linux/macOS:  source venv/bin/activate

pip install "pyspark[connect]==4.0.0" "pandas<3.0"
```

Probá la conexión:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
spark.range(5).show()
```

Si muestra los números del 0 al 4, todo funciona. Si falla justo después de levantar los contenedores, esperá unos segundos: el servidor Spark tarda en estar listo.

## Paso 5. Leer los datos y guardarlos como Parquet

Las rutas HDFS usan `namenode:9000` porque las resuelve el contenedor de Spark, no tu PC:

```python
sdf = spark.read.csv("hdfs://namenode:9000/data/raw/mi_dataset/", header=True, inferSchema=True)
sdf.printSchema()
sdf.show(5)

# Convertir una sola vez a Parquet: más pequeño y mucho más rápido de leer
sdf.write.mode("overwrite").parquet("hdfs://namenode:9000/data/processed/mi_dataset")

# A partir de ahora, leer desde Parquet
sdf = spark.read.parquet("hdfs://namenode:9000/data/processed/mi_dataset")
```

Como Spark corre en Linux dentro de Docker, la escritura funciona sin configurar `winutils` ni `HADOOP_HOME`, que son necesarios con Spark nativo en Windows.

## Buenas prácticas con DataFrames

- Procesá en Spark (`filter`, `groupBy`, `join`, `sql`) y convertí a pandas **solo resultados pequeños** para mostrarlos.
- Evitá `toPandas()` y `collect()` sobre el dataset completo: trae todo a la memoria de tu PC.
- Usá siempre `limit()` o una agregación antes de convertir:

```python
resumen = sdf.groupBy("columna").count().orderBy("count", ascending=False).limit(50)
resumen.toPandas()
```

## Uso diario

| Acción | Comando |
|---|---|
| Encender | `docker compose up -d` |
| Apagar conservando los datos | `docker compose down` |
| Ver el estado | `docker compose ps` |
| Ver logs en vivo | `docker compose logs -f spark` |
| Reiniciar solo Spark | `docker compose restart spark` |
| Abrir una terminal en el namenode | `docker exec -it namenode bash` |
| Listar HDFS completo | `docker exec namenode hdfs dfs -ls -R -h /` |

Los datos de HDFS viven en volúmenes de Docker y se conservan al apagar. **Solo se pierden con `docker compose down -v`.**

## Empezar de cero

Esto elimina contenedores, volúmenes y todo lo subido a HDFS. Tus archivos originales en `datasets/` no se tocan.

```bash
docker compose down -v --remove-orphans
docker ps -a            # verificar que no queden contenedores
docker volume ls        # verificar que no queden volúmenes
docker compose up -d
```

Para borrar también las imágenes descargadas, agregá `--rmi all` al primer comando. Si Docker responde `Resource is still in use`, hay contenedores viejos que las usan: eliminalos con `docker rm -f namenode datanode spark`.

## Configuración

| Qué cambiar | Dónde |
|---|---|
| Nombre del proyecto (prefijo de los volúmenes) | `name:` al inicio del compose |
| Nombre del clúster HDFS | `CLUSTER_NAME` en `namenode` (solo se aplica al formatear un volumen vacío) |
| Memoria de Spark | `--driver-memory` en el comando del servicio `spark` |
| Versión de Spark | Tag de la imagen `apache/spark:X.Y.Z` y versión de `pyspark` en el cliente (deben coincidir) |
| Puertos | Número de la izquierda en `ports`, por ejemplo `"9871:9870"` |
| Carpeta de datos de tu PC | Volumen `./datasets:/datasets` del servicio `namenode` |

Docker usa el campo `name:` (o, si falta, el nombre de la carpeta) como prefijo de los volúmenes. Si lo cambiás o renombrás la carpeta, Docker crea volúmenes nuevos y vacíos, y los anteriores quedan huérfanos. Por eso conviene dejar `name:` fijo.

### Varios clústeres a la vez

El compose usa `container_name` fijo, así que solo puede haber un clúster activo. Para tener varios simultáneos, quitá las líneas `container_name` y cambiá los puertos de la izquierda en cada copia.

## Solución de problemas

| Síntoma | Causa y solución |
|---|---|
| `open //./pipe/dockerDesktopLinuxEngine: El sistema no puede encontrar el archivo` | Docker Desktop no está corriendo. Abrilo y esperá a **Engine running**. |
| `request returned Internal Server Error for API route` | Motor de Docker en mal estado. Reiniciá Docker Desktop, ejecutá `wsl --shutdown` y volvé a abrirlo. Si persiste, reiniciá la PC. |
| `docker exec namenode ls /datasets` sale vacío | La carpeta no se montó. Revisá la ruta del volumen y que la unidad esté permitida en *Settings → Resources → File sharing*. Luego `docker compose up -d --force-recreate namenode`. |
| 0 datanodes en http://localhost:9870 | Todavía arrancando. Esperá un minuto. Si sigue, revisá `docker compose logs datanode`. |
| `UNABLE_TO_INFER_SCHEMA` al leer CSV | La carpeta de HDFS está vacía o la ruta es incorrecta. Verificá con `hdfs dfs -ls -R -h /data`. |
| Error de conexión a `sc://localhost:15002` | Spark aún no está listo o se cayó. Revisá `docker compose logs spark`. |
| `PACKAGE_NOT_INSTALLED ... zstandard` o errores de `grpc` / `protobuf` | Dependencias del cliente en conflicto. Usá un entorno virtual limpio con `pip install "pyspark[connect]==4.0.0"`. |
| `Conflict. The container name "/namenode" is already in use` | Quedó un contenedor viejo. Ejecutá `docker rm -f namenode datanode spark`. |
| `port is already allocated` | Otro programa usa ese puerto. Cambiá el número de la izquierda en `ports`. |
| Docker o Spark muy lentos, `OutOfMemoryError` | Poca memoria. Bajá `--driver-memory`, cerrá otras aplicaciones o limitá WSL2 con `memory=8GB` en `%UserProfile%\.wslconfig` y ejecutá `wsl --shutdown`. |
| Advertencias sobre pandas 3.0 | PySpark todavía no lo soporta por completo. Usá `pandas<3.0`. |

## Notas y limitaciones

- Las imágenes `bde2020/hadoop-*` usan Hadoop 3.2.1 y ya no reciben actualizaciones. Funcionan bien para un nodo de desarrollo. Spark 4 incluye un cliente de Hadoop más nuevo, algo que normalmente es compatible.
- Con un solo datanode la replicación está en 1, por lo que no hay redundancia de datos.
- La seguridad está desactivada (`dfs.permissions.enabled=false`). No expongas estos puertos a una red pública.
- HDFS está pensado para pocos archivos grandes. Muchos archivos de pocos MB afectan el rendimiento.

## Documentación oficial

- Spark SQL y DataFrames: https://spark.apache.org/docs/latest/sql-programming-guide.html
- Referencia de la API de PySpark: https://spark.apache.org/docs/latest/api/python/reference/index.html
- Spark Connect: https://spark.apache.org/docs/latest/spark-connect-overview.html
- Comandos de HDFS: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/FileSystemShell.html
- Docker Compose: https://docs.docker.com/compose/
