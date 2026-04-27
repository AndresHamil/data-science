import os
import sys

from pyspark.sql import SparkSession


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder.appName("LimpiezaDeDatos").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

base_dir = os.path.dirname(__file__)
csv_path = os.path.join(base_dir, "datos", "empleados.csv")

df = spark.read.csv(csv_path, header=True, inferSchema=True)

os.system("cls" if os.name == "nt" else "clear")

print("-------------------------------------")
print("Ejercicio 2 — Limpieza de datos")
print("-------------------------------------")
print(f"Archivo origen: {csv_path}\n")
print("DataFrame original\n")
df.show()
print("-------------------------------------")
print("Eliminar nulos\n")
df.dropna().show()
print("-------------------------------------")
print("Rellenar nulos con 0 o texto\n")
df.fillna({"edad": 0, "salario": 0, "ciudad": "Sin ciudad"}).show()
print("-------------------------------------")
print("Eliminar duplicados\n")
df.dropDuplicates().show()
print("-------------------------------------")

spark.stop()

if sys.stdin.isatty():
	input("Presiona Enter para cerrar...")