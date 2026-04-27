import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


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
print("Ejercicio 3 — Selección de columnas")
print("-------------------------------------")
print("Mostrar todas las columnas\n")
df.show()
print("-------------------------------------")
print("Nombre y salario\n")
df.select("nombre", "salario").show()
print("-------------------------------------")
print("Solo edad\n")
df.select("edad").show()
print("-------------------------------------")
print("Nombre, edad y ciudad\n")
df.select("nombre", "edad", "ciudad").show()
print("-------------------------------------")
print("Solo ciudad\n")
df.select("ciudad").show()
print("-------------------------------------")
print("Nombre y ciudad\n")
df.select("nombre", "ciudad").show()
print("-------------------------------------")
print("Edad y salario\n")
df.select("edad", "salario").show()
print("-------------------------------------")
print("Seleccion con col()\n")
df.select(col("nombre"), col("salario")).show()
print("-------------------------------------")
print("Columnas con alias\n")
df.select(
    col("nombre").alias("empleado"),
    col("salario").alias("sueldo")
).show()
print("-------------------------------------")
print("Seleccion sin repetir filas de ciudad\n")
df.select("ciudad").distinct().show()
print("-------------------------------------")

spark.stop()

if sys.stdin.isatty():
    input("Presiona Enter para cerrar...")