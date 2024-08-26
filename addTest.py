import pandas as pd
import random
import datetime

def generate_random_data(num_records):
    # Lista de nombres y apellidos (puedes ampliarla)
    first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Olivia', ...]
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Miller', ...]

    data = []
    for _ in range(num_records):
        record = {
            "datetime": datetime.datetime(
                random.randint(2020, 2023),  # Año
                random.randint(1, 12),    # Mes
                random.randint(1, 28),    # Día
                random.randint(0, 23),    # Hora
                random.randint(0, 59),    # Minuto
                random.randint(0, 59)     # Segundo
            ).isoformat() + 'Z',
            "department_id": random.randint(1, 10),
            "id": random.randint(100, 1000),
            "job_id": random.randint(100, 200),
            "name": f"{random.choice(first_names)} {random.choice(last_names)}"
        }
        data.append(record)

    df = pd.DataFrame(data)
    return df

# Generar 1200 registros
df = generate_random_data(1200)

# Exportar a CSV
df.to_csv('datos_aleatorios.csv', index=False)