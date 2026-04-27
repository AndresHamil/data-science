import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, max, min, sum


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder.appName("Agrupaciones").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

data = [
	("Luis", 25, "Monterrey", 10000),
	("Ana", 30, "CDMX", 15000),
	("Pedro", 22, "Monterrey", 8000),
	("Sofia", 28, "CDMX", 12000),
	("Juan", 35, "Guadalajara", 20000),
	("Maria", 27, "Monterrey", 11000),
	("Carlos", 40, "Guadalajara", 18000)
]

df = spark.createDataFrame(data, ["nombre", "edad", "ciudad", "salario"])

os.system("cls" if os.name == "nt" else "clear")

print("-------------------------------------")
print("Ejercicio 5 — Agrupaciones")
print("-------------------------------------")
print("DataFrame original\n")
df.show()
print("-------------------------------------")
print("Cantidad de personas por ciudad\n")
df.groupBy("ciudad").count().show()
print("-------------------------------------")
print("Salario promedio por ciudad\n")
df.groupBy("ciudad").avg("salario").show()
print("-------------------------------------")
print("Edad promedio por ciudad\n")
df.groupBy("ciudad").avg("edad").show()
print("-------------------------------------")
print("Suma de salarios por ciudad\n")
df.groupBy("ciudad").sum("salario").show()
print("-------------------------------------")
print("Salario maximo por ciudad\n")
df.groupBy("ciudad").max("salario").show()
print("-------------------------------------")
print("Salario minimo por ciudad\n")
df.groupBy("ciudad").min("salario").show()
print("-------------------------------------")
print("Varias agrupaciones con alias\n")
df.groupBy("ciudad").agg(
	count("nombre").alias("total_personas"),
	avg("edad").alias("promedio_edad"),
	avg("salario").alias("promedio_salario"),
	max("salario").alias("salario_maximo"),
	min("salario").alias("salario_minimo"),
	sum("salario").alias("salario_total")
).show()
print("-------------------------------------")
print("Agrupacion ordenada por salario total\n")
df.groupBy("ciudad").agg(
	sum("salario").alias("salario_total")
).orderBy("salario_total", ascending=False).show()
print("-------------------------------------")
print("Agrupacion por ciudad filtrando salario mayor a 10000\n")
df.filter(df["salario"] > 10000).groupBy("ciudad").count().show()
print("-------------------------------------")

spark.stop()

if sys.stdin.isatty():
	input("Presiona Enter para cerrar...")
