import os
import sys
import tempfile

from pyspark.sql import SparkSession


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder.appName("LecturaYEscrituraDeArchivos").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

temp_dir = tempfile.mkdtemp(prefix="pyspark_archivos_")
base_dir = os.path.dirname(__file__)
csv_path = os.path.join(base_dir, "datos", "empleados.csv")
salida_csv = os.path.join(temp_dir, "salida_csv")
salida_parquet = os.path.join(temp_dir, "datos.parquet")

df = spark.read.csv(csv_path, header=True, inferSchema=True)

df.write.mode("overwrite").csv(salida_csv, header=True)
df.write.mode("overwrite").parquet(salida_parquet)

os.system("cls" if os.name == "nt" else "clear")

print("------------------------------------------------------")
print("Ejercicio 6 — Lectura y escritura de archivos")
print("------------------------------------------------------")
print(f"Archivo leido: {csv_path}\n")
df.show()
print("------------------------------------------------------")
print("Archivo CSV guardado en\n")
print(salida_csv)
print("------------------------------------------------------")
print("Archivo Parquet guardado en\n")
print(salida_parquet)
print("------------------------------------------------------")

spark.stop()

if sys.stdin.isatty():
	input("Presiona Enter para cerrar...")