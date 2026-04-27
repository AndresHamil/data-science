import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, upper, when


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder.appName("CrearNuevasColumnas").getOrCreate()
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
print("Ejercicio 4 — Crear nuevas columnas")
print("-------------------------------------")
print("DataFrame original\n")
df.show()
print("-------------------------------------")
print("Agregar columna salario_anual\n")
df.withColumn("salario_anual", col("salario") * 12).show()
print("-------------------------------------")
print("Agregar columna es_mayor_30\n")
df.withColumn("es_mayor_30", col("edad") > 30).show()
print("-------------------------------------")
print("Agregar columna bono del 10%\n")
df.withColumn("bono", col("salario") * 0.10).show()
print("-------------------------------------")
print("Agregar columna salario_total con bono\n")
df.withColumn("salario_total", col("salario") * 1.10).show()
print("-------------------------------------")
print("Agregar columna categoria_edad\n")
df.withColumn(
	"categoria_edad",
	when(col("edad") < 25, "Joven").when(col("edad") <= 30, "Adulto").otherwise("Mayor")
).show()
print("-------------------------------------")
print("Agregar columna ciudad_pais\n")
df.withColumn("ciudad_pais", concat(col("ciudad"), lit(", Mexico"))).show(truncate=False)
print("-------------------------------------")
print("Agregar columna nombre_en_mayusculas\n")
df.withColumn("nombre_en_mayusculas", upper(col("nombre"))).show()
print("-------------------------------------")
print("Agregar dos columnas nuevas juntas\n")
df.withColumn("salario_anual", col("salario") * 12) \
  .withColumn("es_mayor_30", col("edad") > 30) \
  .show()
print("-------------------------------------")

spark.stop()

if sys.stdin.isatty():
	input("Presiona Enter para cerrar...")
