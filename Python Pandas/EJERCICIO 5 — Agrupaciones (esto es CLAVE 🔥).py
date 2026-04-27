import pandas as pd

print("-------------------------------------")
print("Ejercicio 5 — Agrupaciones (esto es CLAVE 🔥)")

data = {
    "nombre": ["Luis", "Ana", "Pedro", "Sofia", "Juan"],
    "edad": [25, 30, 22, 28, 35],
    "ciudad": ["Monterrey", "CDMX", "Monterrey", "CDMX", "Guadalajara"],
    "salario": [10000, 15000, 8000, 12000, 20000]
}

df = pd.DataFrame(data)

print("-------------------------------------")
print("Salario promedio por ciudad\n")
print(df.groupby("ciudad")["salario"].mean())
print("-------------------------------------")
print("Edad promedio por ciudad\n")
print(df.groupby("ciudad")["edad"].mean())
print("-------------------------------------")
print("Número de personas por ciudad\n")
print(df.groupby("ciudad")["nombre"].count())
print("-------------------------------------")
print("Salario máximo por ciudad\n")
print(df.groupby("ciudad")["salario"].max())
print("-------------------------------------")
print("Salario mínimo por ciudad\n")    
print(df.groupby("ciudad")["salario"].min())
print("-------------------------------------")
print("Salario total por ciudad\n")
print(df.groupby("ciudad")["salario"].sum())
print("-------------------------------------")
print("Edad máxima por ciudad\n")
print(df.groupby("ciudad")["edad"].max())
print("-------------------------------------")
print("Edad mínima por ciudad\n")
print(df.groupby("ciudad")["edad"].min())
print("-------------------------------------")
print("Número de personas mayores de 30 años por ciudad\n")
print(df[df["edad"] > 30].groupby("ciudad")["nombre"].count())
print("-------------------------------------")