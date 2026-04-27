import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder.appName("AgrupacionesPRO").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

base_dir = os.path.dirname(__file__)
csv_path = os.path.join(base_dir, "datos", "empleados.csv")

df = spark.read.csv(csv_path, header=True, inferSchema=True).dropna(subset=["salario"])

os.system("cls" if os.name == "nt" else "clear")

print("-------------------------------------")
print("Ejercicio 4 — Agrupaciones PRO")
print("-------------------------------------")
print(f"Archivo origen: {csv_path}\n")
print("DataFrame original\n")
df.show()
print("-------------------------------------")
print("Promedio salario por ciudad\n")
df.groupBy("ciudad").avg("salario").show()
print("-------------------------------------")
print("Conteo por ciudad\n")
df.groupBy("ciudad").count().show()
print("-------------------------------------")
print("Maximo salario por ciudad\n")
df.groupBy("ciudad").max("salario").show()
print("-------------------------------------")
print("Resumen profesional ordenado\n")
df.groupBy("ciudad").agg(
	avg("salario").alias("promedio_salario"),
	max("salario").alias("salario_maximo")
).orderBy("promedio_salario", ascending=False).show()
print("-------------------------------------")

spark.stop()

if sys.stdin.isatty():
	input("Presiona Enter para cerrar...")