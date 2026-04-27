import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder.appName("Joins").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

base_dir = os.path.dirname(__file__)
clientes_path = os.path.join(base_dir, "datos", "clientes.csv")
ventas_path = os.path.join(base_dir, "datos", "ventas.csv")

df_clientes = spark.read.csv(clientes_path, header=True, inferSchema=True)
df_ventas = spark.read.csv(ventas_path, header=True, inferSchema=True)

df_join = df_clientes.join(df_ventas, on="id", how="inner")

os.system("cls" if os.name == "nt" else "clear")

print("-------------------------------------")
print("Ejercicio 5 — Joins")
print("-------------------------------------")
print(f"Archivo clientes: {clientes_path}")
print(f"Archivo ventas: {ventas_path}\n")
print("Clientes\n")
df_clientes.show()
print("-------------------------------------")
print("Ventas\n")
df_ventas.show()
print("-------------------------------------")
print("Join entre clientes y ventas\n")
df_join.show()
print("-------------------------------------")
print("Ventas por cliente\n")
df_join.groupBy("nombre").agg(sum("monto").alias("total_ventas")).orderBy(
	"total_ventas",
	ascending=False
).show()
print("-------------------------------------")

spark.stop()

if sys.stdin.isatty():
	input("Presiona Enter para cerrar...")