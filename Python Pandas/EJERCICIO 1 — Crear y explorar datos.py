import pandas as pd

print("-------------------------------------")
print("Ejercicio 1 — Crear y explorar datos")

data = {
    "nombre": ["Luis", "Ana", "Pedro", "Sofia", "Juan"],
    "edad": [25, 30, 22, 28, 35],
    "ciudad": ["Monterrey", "CDMX", "Monterrey", "CDMX", "Guadalajara"],
    "salario": [10000, 15000, 8000, 12000, 20000]
}

df = pd.DataFrame(data)

print("-------------------------------------")
print("Primeras filas del DataFrame\n")
print(df.head())
print("-------------------------------------")
print("Información del DataFrame\n")
df.info()
print("-------------------------------------")
print("Estadísticas descriptivas\n")
print(df.describe())
print("-------------------------------------")
print("Personas mayores de 25 años\n")
print(df[df["edad"] > 25])
print("-------------------------------------")