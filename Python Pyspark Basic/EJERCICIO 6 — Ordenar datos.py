import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder.appName("OrdenarDatos").getOrCreate()
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
print("Ejercicio 6 — Ordenar datos")
print("-------------------------------------")
print("DataFrame original\n")
df.show()
print("-------------------------------------")
print("Ordenar por salario DESC\n")
df.orderBy(col("salario").desc()).show()
print("-------------------------------------")
print("Ordenar por edad\n")
df.orderBy(col("edad")).show()
print("-------------------------------------")
print("Ordenar por salario ASC\n")
df.orderBy(col("salario").asc()).show()
print("-------------------------------------")
print("Ordenar por edad DESC\n")
df.orderBy(col("edad").desc()).show()
print("-------------------------------------")
print("Ordenar por nombre A-Z\n")
df.orderBy(col("nombre").asc()).show()
print("-------------------------------------")
print("Ordenar por ciudad y luego por salario DESC\n")
df.orderBy(col("ciudad").asc(), col("salario").desc()).show()
print("-------------------------------------")
print("Ordenar por ciudad y luego por edad\n")
df.orderBy(col("ciudad").asc(), col("edad").asc()).show()
print("-------------------------------------")
print("Top 3 salarios mas altos\n")
df.orderBy(col("salario").desc()).show(3)
print("-------------------------------------")
print("Ordenar seleccionando solo nombre y salario\n")
df.select("nombre", "salario").orderBy(col("salario").desc()).show()
print("-------------------------------------")
spark.stop()

if sys.stdin.isatty():
	input("Presiona Enter para cerrar...")
