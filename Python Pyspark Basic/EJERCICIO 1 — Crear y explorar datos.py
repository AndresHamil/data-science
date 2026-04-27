import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder.appName("CrearYExplorarDatos").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

data = [
    ("Luis", 25, "Monterrey", 10000),
    ("Ana", 30, "CDMX", 15000),
    ("Pedro", 22, "Monterrey", 8000),
    ("Sofia", 28, "CDMX", 12000),
    ("Juan", 35, "Guadalajara", 20000)
]

df = spark.createDataFrame(data, ["nombre", "edad", "ciudad", "salario"])

os.system("cls" if os.name == "nt" else "clear")

print("-------------------------------------")
print("Ejercicio 1 — Crear y explorar datos")
print("-------------------------------------")
print("Primeras filas del DataFrame\n")
df.show(5, truncate=False)
print("-------------------------------------")
print("Estructura del DataFrame\n")
df.printSchema()
print("-------------------------------------")
print("Estadísticas descriptivas\n")
df.describe().show()
print("-------------------------------------")
print("Personas mayores de 25 años\n")
df.filter(df["edad"] > 25).show()
print("-------------------------------------")
print("Total de registros\n")
print(df.count())
print("-------------------------------------")
print("Solo nombre y salario\n")
df.select("nombre", "salario").show()
print("-------------------------------------")
print("Personas que viven en CDMX\n")
df.filter(df["ciudad"] == "CDMX").show()
print("-------------------------------------")
print("Ordenar por salario de mayor a menor\n")
df.orderBy(df["salario"].desc()).show()
print("-------------------------------------")
print("Promedio de salario por ciudad\n")
df.groupBy("ciudad").agg(avg("salario").alias("promedio_salario")).show()
print("-------------------------------------")

spark.stop()

if sys.stdin.isatty():
    input("Presiona Enter para cerrar...")