import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, sum


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder.appName("MiniProyecto").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

base_dir = os.path.dirname(__file__)
clientes_path = os.path.join(base_dir, "datos", "clientes.csv")
ventas_path = os.path.join(base_dir, "datos", "ventas.csv")

df_clientes = spark.read.csv(clientes_path, header=True, inferSchema=True)
df_ventas = spark.read.csv(ventas_path, header=True, inferSchema=True)
df = df_clientes.join(df_ventas, on="id", how="inner").select(
	col("nombre").alias("cliente"),
	col("monto")
)

ventas_por_cliente = df.groupBy("cliente").agg(sum("monto").alias("total_ventas"))
cliente_top = ventas_por_cliente.orderBy(col("total_ventas").desc()).first()
promedio_ventas = df.agg(avg("monto").alias("promedio_ventas")).first()

os.system("cls" if os.name == "nt" else "clear")

print("-------------------------------------------")
print("Ejercicio 7 — Mini proyecto")
print("-------------------------------------------")
print(f"Archivo clientes: {clientes_path}")
print(f"Archivo ventas: {ventas_path}\n")
print("Ventas originales\n")
df.show()
print("-------------------------------------------")
print("Total ventas por cliente\n")
ventas_por_cliente.orderBy(col("total_ventas").desc()).show()
print("-------------------------------------------")
print("Cliente que mas compro\n")
print(f"{cliente_top['cliente']} con un total de {cliente_top['total_ventas']}")
print("-------------------------------------------")
print("Promedio de ventas\n")
print(f"{promedio_ventas['promedio_ventas']:.2f}")
print("-------------------------------------------")

spark.stop()

if sys.stdin.isatty():
	input("Presiona Enter para cerrar...")