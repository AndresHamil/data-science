import pandas as pd

print("-------------------------------------")
print("Ejercicio 2 — Filtros")

data = {
    "nombre": ["Luis", "Ana", "Pedro", "Sofia", "Juan","Miguel"],
    "edad": [25, 30, 22, 28, 35,40],
    "ciudad": ["Monterrey", "CDMX", "Monterrey", "CDMX", "Guadalajara","Monterrey"],
    "salario": [10000, 15000, 8000, 12000, 20000,25000]
}

df = pd.DataFrame(data)

print("-------------------------------------")
print("Mayores de 25 años\n")
print(df[df["edad"] > 25])
print("-------------------------------------")
print("Mayores de 25 años y viven en Monterrey\n")
print(df[(df["edad"] > 25) & (df["ciudad"] == "Monterrey")])
print("-------------------------------------")
print("Salario mayor a 15000 o viven en CDMX\n")
print(df[(df["salario"] > 15000) | (df["ciudad"] == "CDMX")])
print("-------------------------------------")
print("No viven en CDMX\n")
print(df[~(df["ciudad"] == "CDMX")])
print("-------------------------------------")
print("Edad entre 25 y 35 años\n")
print(df[df["edad"].between(25, 35)])
print("-------------------------------------")
print("Nombres que empiezan con 'J'\n")
print(df[df["nombre"].str.startswith("J")])
print("-------------------------------------")
print("Nombres que terminan con 'a'\n")
print(df[df["nombre"].str.endswith("a")])
print("-------------------------------------")
print("Nombres que contienen 'e'\n")
print(df[df["nombre"].str.contains("e")])
print("-------------------------------------")
print("Nombres que no contienen 'e'\n")
print(df[~df["nombre"].str.contains("e")])
print("-------------------------------------")
print("Nombres que contienen 'e' y viven en Monterrey\n")
print(df[df["nombre"].str.contains("e") & (df["ciudad"] == "Monterrey")])
print("-------------------------------------")
print("Nombres que contienen 'e' o viven en Monterrey\n")
print(df[df["nombre"].str.contains("e") | (df["ciudad"] == "Monterrey")])
print("-------------------------------------")
print("Nombres que contienen 'e' pero no viven en Monterrey\n")
print(df[df["nombre"].str.contains("e") & ~(df["ciudad"] == "Monterrey")])
print("-------------------------------------")
print("Nombres que no contienen 'e' y viven en Monterrey\n")
print(df[~df["nombre"].str.contains("e") & (df["ciudad"] == "Monterrey")])
print("-------------------------------------")
print("Nombres que no contienen 'e' o viven en Monterrey\n")
print(df[~df["nombre"].str.contains("e") | (df["ciudad"] == "Monterrey")])
print("-------------------------------------")  
