import pandas as pd

print("-------------------------------------")
print("Ejercicio 3 — Selección de columnas")

data = {
    "nombre": ["Luis", "Ana", "Pedro", "Sofia", "Juan"],
    "edad": [25, 30, 22, 28, 35],
    "ciudad": ["Monterrey", "CDMX", "Monterrey", "CDMX", "Guadalajara"],
    "salario": [10000, 15000, 8000, 12000, 20000]
}

df = pd.DataFrame(data)

print("-------------------------------------")
print("Seleccionar la columna 'nombre'\n")
print(df["nombre"])
print("-------------------------------------")
print("Seleccionar las columnas 'nombre' y 'salario'\n")
print(df[["nombre", "salario"]]) 
print("-------------------------------------")       
print("Seleccionar la columna 'edad' para las personas que viven en CDMX\n")
print(df.loc[df["ciudad"] == "CDMX", "edad"])
print("-------------------------------------")
print("Seleccionar las columnas 'nombre' y 'ciudad' para las personas con salario mayor a 10000\n")
print(df.loc[df["salario"] > 10000, ["nombre", "ciudad"]])
print("-------------------------------------")
print("Seleccionar la columna 'salario' para las personas menores de 30 años\n")
print(df.loc[df["edad"] < 30, "salario"])
print("-------------------------------------")
print("Seleccionar las columnas 'nombre' y 'salario' para las personas que viven en Monterrey y tienen salario mayor a 15000\n")
print(df.loc[(df["ciudad"] == "Monterrey") & (df["salario"] > 15000), ["nombre", "salario"]])
print("-------------------------------------")
print("Seleccionar la columna 'ciudad' para las personas con edad entre 25 y 35 años\n")
print(df.loc[df["edad"].between(25, 35), "ciudad"])
print("-------------------------------------")
print("Seleccionar las columnas 'nombre' y 'edad' para las personas que viven en CDMX o tienen salario mayor a 12000\n")
print(df.loc[(df["ciudad"] == "CDMX") | (df["salario"] > 12000), ["nombre", "edad"]])
print("-------------------------------------")
print("Seleccionar la columna 'nombre' para las personas que no viven en CDMX\n")
print(df.loc[~(df["ciudad"] == "CDMX"), "nombre"])
print("-------------------------------------")
print("Seleccionar las columnas 'nombre' y 'ciudad' para las personas que viven en CDMX y tienen edad menor a 30 años\n")
print(df.loc[(df["ciudad"] == "CDMX") & (df["edad"] < 30), ["nombre", "ciudad"]])
print("-------------------------------------")
print("Seleccionar la columna 'salario' para las personas que no viven en CDMX y tienen salario mayor a 15000\n")
print(df.loc[~(df["ciudad"] == "CDMX") & (df["salario"] > 15000), "salario"])
print("-------------------------------------")
