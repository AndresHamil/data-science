import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder.appName("MiniReto").getOrCreate()
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
print("Ejercicio 7 — Mini reto")
print("-------------------------------------")
print("DataFrame original\n")
df.show()
print("-------------------------------------")

print("Persona con mayor salario\n")
persona_mayor_salario = df.orderBy(col("salario").desc()).first()
print(f"Quien gana mas: {persona_mayor_salario['nombre']} con salario de {persona_mayor_salario['salario']}")
print("-------------------------------------")

print("Ciudad con mayor salario promedio\n")
ciudad_mayor_promedio = df.groupBy("ciudad").agg(
	avg("salario").alias("promedio_salario")
).orderBy(col("promedio_salario").desc()).first()
print(
	f"La ciudad con mayor salario promedio es {ciudad_mayor_promedio['ciudad']} "
	f"con promedio de {ciudad_mayor_promedio['promedio_salario']:.2f}"
)
print("-------------------------------------")

print("Cantidad de personas por ciudad\n")
personas_por_ciudad = df.groupBy("ciudad").agg(count("nombre").alias("total_personas"))
personas_por_ciudad.show()
print("-------------------------------------")

print("Resumen final del reto\n")
print(f"1. Quien gana mas: {persona_mayor_salario['nombre']}")
print(f"2. Ciudad con mayor salario promedio: {ciudad_mayor_promedio['ciudad']}")
print("3. Cuantas personas hay por ciudad:")

for fila in personas_por_ciudad.orderBy("ciudad").collect():
	print(f"   - {fila['ciudad']}: {fila['total_personas']}")

print("-------------------------------------")

spark.stop()

if sys.stdin.isatty():
	input("Presiona Enter para cerrar...")
