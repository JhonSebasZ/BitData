import happybase
import pandas as pd
from datetime import datetime
# Bloque principal de ejecución
try:
    # 1. Establecer conexión con HBase
    connection = happybase.Connection('localhost')
    print("Conexión establecida con HBase")
    # 2. Crear la tabla con las familias de columnas
    table_name = 'used_mobile'
    families = {
    'mobile': dict(), # información básica del mobile
    'tiempo': dict(), # especifica el tiempo que se pasa el el celular
    'persona': dict(), # información basica de la persona
    }

    # Eliminar la tabla si ya existe
    if table_name.encode() in connection.tables():
       print(f"Eliminando tabla existente - {table_name}")
       connection.delete_table(table_name, disable=True)

    # Crear nueva tabla
    connection.create_table(table_name, families)
    table = connection.table(table_name)
    print(f"Tabla '{table_name}' creada exitosamente")

    # 3. Cargar datos del CSV
    mobile_data = pd.read_csv('uso_dispositivos_moviles')
 
    # Iterar sobre el DataFrame usando el índice
    for index, row in mobile_data.iterrows():
       # Generar row key basado en el índice
       row_key = f'mobile_{index}'.encode()
    
       # Organizar los datos en familias de columnas
       data = {
               b'mobile:device_model': str(row['Device Model']).encode(),
               b'mobile:operating_system': str(row['Operating System']).encode(),
               b'mobile:number_apps_installed': str(row['Number of Apps Installed']).encode(),

               b'tiempo:app_usege_time': str(row['App Usage Time (min/day)']).encode(),
               b'tiempo:screen_on_time': str(row['Screen On Time (hours/day)']).encode(),

               b'persona:gender': str(row['Gender']).encode(),
               b'persona:age': str(row['Age']).encode(),
           }

       table.put(row_key, data)
    
       print("Datos cargados exitosamente")

    # 4. Consultas y Análisis de Datos
    print("\n=== Todos los celulares en la base de datos (primeros 3) ===")
    count = 0
    for key, data in table.scan():
       if count < 3: # Limitamos a 3 para el ejemplo
           print(f"\nCelular ID: {key.decode()}")
           print(f"Modelo: {data[b'mobile:device_model'].decode()}")
           print(f"OS: {data[b'mobile:operating_system'].decode()}")
           count += 1

    # 6. Encontrar celulares por rango de edades de las personas que los usan
    print("\n=== celulares que son usados por personas menores a 20 años ===")
    for key, data in table.scan():
       if int(data[b'persona:age'].decode()) < 20:
           print(f"\Persona ID: {key.decode()}")
           print(f"Modelo: {data[b'mobile:device_model'].decode()}")
           print(f"OS: {data[b'mobile:operating_system'].decode()}")

    # 7. Análisis de sistemas operativos
    print("\n=== celulares por sistema operativo ===")
    os = {}
    for key, data in table.scan():
       o_s = data[b'mobile:operating_system'].decode()
       os[o_s] = os.get(o_s, 0) + 1
    
    for o_s, count in os.items():
       print(f"{o_s}: {count} celulares")

    # 8. Top 3 celulares mas mayor numero de app instaladas
    print("\n=== Top 3 celulares con mayor numero de app instaladas ===")
    celulares_by_app = []
    for key, data in table.scan():
       celulares_by_app.append({
       'id': key.decode(),
       'modelo': data[b'mobile:device_model'].decode(),
       'os': data[b'mobile:operating_system'].decode(),
       'app': int(data[b'mobile:number_apps_installed'].decode()),
       })
 
    for mobile in sorted(celulares_by_app, key=lambda x: x['app'], reverse=True)[:3]:
       print(f"ID: {mobile['id']}")
       print(f"modelo: {mobile['modelo']}")
       print(f"os: {mobile['os']}")
       print(f"apps: {mobile['app']}\n")
except Exception as e:
    print(f"Error: {str(e)}")
finally:
    # Cerrar la conexión
    connection.close()