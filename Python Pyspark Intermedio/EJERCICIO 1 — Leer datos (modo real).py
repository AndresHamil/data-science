import os
import sys

from pyspark.sql import SparkSession


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder.appName("LeerDatosModoReal").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

base_dir = os.path.dirname(__file__)
csv_path = os.path.join(base_dir, "datos", "empleados.csv")

df = spark.read.csv(csv_path, header=True, inferSchema=True)

os.system("cls" if os.name == "nt" else "clear")

print("-----------------------------------------------")
print("Ejercicio 1 — Leer datos (modo real)")
print("-----------------------------------------------")
print(f"Archivo simulado: {csv_path}\n")
print("Mostrar datos\n")
df.show(truncate=False)
print("-----------------------------------------------")
print("Ver schema\n")
df.printSchema()
print("-----------------------------------------------")
print("Contar registros\n")
print(df.count())
print("-----------------------------------------------")

spark.stop()

if sys.stdin.isatty():
	input("Presiona Enter para cerrar...")