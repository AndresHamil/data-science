import os
import sys

from pyspark.sql import SparkSession


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
print("Ejercicio 2 — Filtros")
print("-------------------------------------")
print("Personas con salario mayor a 10000\n")
df.filter(df["salario"] > 10000).show()
print("-------------------------------------")
print("Personas que viven en Monterrey\n")
df.filter(df["ciudad"] == "Monterrey").show()
print("-------------------------------------")
print("Personas con edad entre 25 y 30 años\n")
df.filter((df["edad"] >= 25) & (df["edad"] <= 30)).show()
print("-------------------------------------")
print("Edad mayor a 25\n")
df.filter(df["edad"] > 25).show()
print("-------------------------------------")
print("Ciudad igual a CDMX\n")
df.filter(df["ciudad"] == "CDMX").show()
print("-------------------------------------")
print("Salario mayor a 12000\n")
df.filter(df["salario"] > 12000).show()
print("-------------------------------------")
print("Personas de CDMX con salario mayor a 12000\n")
df.filter((df["ciudad"] == "CDMX") & (df["salario"] > 12000)).show()
print("-------------------------------------")
print("Personas que no viven en Monterrey\n")
df.filter(df["ciudad"] != "Monterrey").show()
print("-------------------------------------")
print("Personas de Monterrey o Guadalajara\n")
df.filter(df["ciudad"].isin("Monterrey", "Guadalajara")).show()
print("-------------------------------------")

spark.stop()

if sys.stdin.isatty():
    input("Presiona Enter para cerrar...")