import json
import random
import datetime

def generar_datos_aleatorios():
    """Genera un diccionario con datos aleatorios."""
    datetime_str = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    department_id = random.randint(1, 10)
    id = random.randint(1, 1000)
    job_id = random.randint(1, 100)
    name = f"Persona {id}"

    data = {
        "datetime": datetime_str,
        "department_id": department_id,
        "id": id,
        "job_id": job_id,
        "name": name
    }

    return data

# Genera 1050 objetos JSON
datos_json = [generar_datos_aleatorios() for _ in range(1050)]

# Especifica la ruta del archivo donde quieres guardar los datos
ruta_archivo = "datos.json"

# Abre el archivo en modo escritura y guarda los datos en formato JSON
with open(ruta_archivo, "w") as archivo:
    json.dump(datos_json, archivo, indent=4)

print(f"Se han generado y guardado {len(datos_json)} registros en {ruta_archivo}")
