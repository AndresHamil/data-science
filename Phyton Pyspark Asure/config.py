import os


"""
Archivo de configuracion del proyecto.

Aqui se define desde donde se leen los datos y donde se escriben los resultados.
La idea es separar las rutas de la logica del pipeline.

Relacion con otros archivos:
- pipeline_utils.py usa este archivo para saber que CSV leer y donde guardar Parquet.
- Los ejercicios no construyen rutas manualmente; le preguntan a config.py.

En local, las rutas apuntan a la carpeta datos del proyecto.
En Azure, estas mismas funciones pueden devolver rutas de Data Lake.
"""


DIRECTORIO_BASE = os.path.dirname(__file__)

# Cambia esta variable manualmente segun el entorno que quieras usar.
# Opciones esperadas: "local" o "azure"
ENTORNO = "local"
ENTORNO_DATOS = ENTORNO.lower()

DIRECTORIO_RAW_LOCAL = os.path.join(DIRECTORIO_BASE, "datos", "raw")
DIRECTORIO_SALIDA_LOCAL = os.path.join(DIRECTORIO_BASE, "datos", "output")

# Ejemplo local real que hoy usa este proyecto:
# G:\My Drive\data-science\Phyton Pyspark Asure\datos\raw\ventas.csv

# Ejemplo ficticio de ruta externa en Azure Data Lake:
# abfss://raw@mi_storage_ficticio.dfs.core.windows.net/ventas/ventas.csv

BASE_RAW_AZURE = "abfss://raw@storageaccount.dfs.core.windows.net/ventas"
BASE_SALIDA_AZURE = "abfss://gold@storageaccount.dfs.core.windows.net/ventas"


def obtener_ruta_entrada(nombre_archivo: str) -> str:
	# Aqui se decide la ruta de entrada segun el entorno activo.
	if ENTORNO_DATOS == "azure":
		return f"{BASE_RAW_AZURE}/{nombre_archivo}"
	return os.path.join(DIRECTORIO_RAW_LOCAL, nombre_archivo)


def obtener_ruta_salida(nombre_carpeta: str) -> str:
	# Aqui se decide la ruta de salida para resultados transformados.
	if ENTORNO_DATOS == "azure":
		return f"{BASE_SALIDA_AZURE}/{nombre_carpeta}"
	return os.path.join(DIRECTORIO_SALIDA_LOCAL, nombre_carpeta)


def resumen_entorno() -> str:
	# Aqui solo devolvemos un texto corto para que los ejercicios muestren en que entorno corren.
	if ENTORNO_DATOS == "azure":
		return "Entorno configurado: Azure Databricks / Data Lake"
	return "Entorno configurado: local"