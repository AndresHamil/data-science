import pandas as pd

print("-------------------------------------")
print("Ejercicio 6 — Ordenar datos")

data = {
    "nombre": ["Luis", "Ana", "Pedro", "Sofia", "Juan"],
    "edad": [25, 30, 22, 28, 35],
    "ciudad": ["Monterrey", "CDMX", "Monterrey", "CDMX", "Guadalajara"],
    "salario": [10000, 15000, 8000, 12000, 20000]
}

df = pd.DataFrame(data)

print("-------------------------------------")
print("Ordenar por edad de menor a mayor\n")
print(df.sort_values("edad"))   
print("-------------------------------------")
print("Ordenar por edad de mayor a menor\n")
print(df.sort_values("edad", ascending=False))
print("-------------------------------------")
print("Ordenar por ciudad y luego por salario\n")
print(df.sort_values(["ciudad", "salario"]))
print("-------------------------------------")
print("Ordenar por ciudad de mayor a menor y luego por salario de menor a mayor\n")
print(df.sort_values(["ciudad", "salario"], ascending=[False, True]))
print("-------------------------------------")
print("Ordenar por salario de menor a mayor y luego por edad de mayor a menor\n")
print(df.sort_values(["salario", "edad"], ascending=[True, False]))
print("-------------------------------------")
print("Ordenar por ciudad de menor a mayor, luego por salario de mayor a menor y luego por edad de menor a mayor\n")
print(df.sort_values(["ciudad", "salario", "edad"], ascending=[True, False, True]))
print("-------------------------------------")
print("Ordenar por ciudad de mayor a menor, luego por salario de menor a mayor y luego por edad de mayor a menor\n")
print(df.sort_values(["ciudad", "salario", "edad"], ascending=[False, True, False]))
print("-------------------------------------")
