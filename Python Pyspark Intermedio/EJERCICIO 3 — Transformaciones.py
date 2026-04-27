import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder.appName("Transformaciones").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

base_dir = os.path.dirname(__file__)
csv_path = os.path.join(base_dir, "datos", "empleados.csv")

df = spark.read.csv(csv_path, header=True, inferSchema=True).dropna(subset=["salario"])

df = df.withColumn("salario_anual", col("salario") * 12)

df = df.withColumn(
	"nivel_salario",
	when(col("salario") > 12000, "Alto").otherwise("Bajo")
)

df = df.withColumn(
	"bono_estimado",
	when(col("salario") > 12000, col("salario") * 0.15).otherwise(col("salario") * 0.08)
)

os.system("cls" if os.name == "nt" else "clear")

print("-------------------------------------")
print("Ejercicio 3 — Transformaciones")
print("-------------------------------------")
print(f"Archivo origen: {csv_path}\n")
print("DataFrame transformado\n")
df.show(truncate=False)
print("-------------------------------------")

spark.stop()

if sys.stdin.isatty():
	input("Presiona Enter para cerrar...")