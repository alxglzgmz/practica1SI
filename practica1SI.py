import json
import sqlite3
import pandas as pd
import statistics
import matplotlib.pyplot as plt

def creacionTablas(con):
    cursorObj = con.cursor()

    cursorObj.execute('''DROP TABLE IF EXISTS alerts''')
    cursorObj.execute('''DROP TABLE IF EXISTS analisis''')
    cursorObj.execute('''DROP TABLE IF EXISTS responsable''')
    cursorObj.execute('''DROP TABLE IF EXISTS devices''')


    cursorObj.execute('''CREATE TABLE IF NOT EXISTS alerts (timestamp timestamp ,sid text ,msg text ,clasificacion text ,prioridad int,protocolo text,origen text ,destino text ,puerto int)''')
    cursorObj.execute('''CREATE TABLE IF NOT EXISTS analisis(id INTEGER PRIMARY KEY, puertosabiertos text, numberports int, servicios int, servicios_inseguros int, vulnerabilidades int)''')
    cursorObj.execute('''CREATE TABLE IF NOT EXISTS responsable(nombre text PRIMARY KEY, telefono text, rol text)''')
    cursorObj.execute('''CREATE TABLE IF NOT EXISTS devices(id text PRIMARY KEY, ip text, localizacion text, nombre_responsable text, analisis_id int, FOREIGN KEY(nombre_responsable) REFERENCES responsable(nombre), FOREIGN KEY(analisis_id) REFERENCES analisis(id))''')


    cursorObj.execute('''INSERT INTO responsable VALUES ("admin", "656445552","Administracion de sistemas") ON CONFLICT(nombre) DO NOTHING''')
    cursorObj.execute('''INSERT INTO responsable VALUES ("Paco Garcia", "640220120","Direccion") ON CONFLICT(nombre) DO NOTHING''')
    cursorObj.execute('''INSERT INTO responsable VALUES ("Luis Sanchez", "None","Desarrollador") ON CONFLICT(nombre) DO NOTHING''')
    cursorObj.execute('''INSERT INTO responsable VALUES ("admin", "656445552","Administracion de sistemas") ON CONFLICT(nombre) DO NOTHING''')
    cursorObj.execute('''INSERT INTO responsable VALUES ("admiin", "None","None") ON CONFLICT(nombre) DO NOTHING''')
    cursorObj.execute('''INSERT INTO responsable VALUES ("admin", "656445552","Administracion de sistemas") ON CONFLICT(nombre) DO NOTHING''')
    cursorObj.execute('''INSERT INTO responsable VALUES ("admin", "656445552","Administracion de sistemas") ON CONFLICT(nombre) DO NOTHING''')


    cursorObj.execute('''INSERT INTO analisis VALUES(1,"80/TCP, 443/TCP, 3306/TCP, 40000/UDP",4,3,0,15) ON CONFLICT(id) DO NOTHING''')
    cursorObj.execute('''INSERT INTO analisis VALUES(2,"None",0,0,0,4) ON CONFLICT(id) DO NOTHING''')
    cursorObj.execute('''INSERT INTO analisis VALUES(3,"1194/UDP, 8080/TCP, 8080/UDP, 40000/UDP",4,1,1,52) ON CONFLICT(id) DO NOTHING''')
    cursorObj.execute('''INSERT INTO analisis VALUES(4,"443/UDP, 80/TCP",2,1,0,3) ON CONFLICT(id) DO NOTHING''')
    cursorObj.execute('''INSERT INTO analisis VALUES(5,"80/TCP, 67/UDP, 68/UDP",3,2,2,12) ON CONFLICT(id) DO NOTHING''')
    cursorObj.execute('''INSERT INTO analisis VALUES(6,"8080/TCP, 3306/TCP, 3306/UDP",3,2,0,2) ON CONFLICT(id) DO NOTHING''')
    cursorObj.execute('''INSERT INTO analisis VALUES(7,"80/TCP, 443/TCP, 9200/TCP, 9300/TCP, 5601/TCP",5,3,2,21) ON CONFLICT(id) DO NOTHING''')


    cursorObj.execute('''INSERT INTO devices VALUES("web","172.18.0.0","None","admin",1)''')
    cursorObj.execute('''INSERT INTO devices VALUES("paco_pc","172.17.0.0","Barcelona","Paco Garcia",2)''')
    cursorObj.execute('''INSERT INTO devices VALUES("luis_pc","172.19.0.0","Madrid","Luis Sanchez",3)''')
    cursorObj.execute('''INSERT INTO devices VALUES("router1","172.1.0.0","None","admin",4)''')
    cursorObj.execute('''INSERT INTO devices VALUES("dhcp_server","172.1.0.1","Madrid","admiin",5)''')
    cursorObj.execute('''INSERT INTO devices VALUES("mysql_db","172.18.0.1","None","admin",6)''')
    cursorObj.execute('''INSERT INTO devices VALUES("ELK","172.18.0.2","None","admin",7)''')



    con.commit()

def ejercicio2(con):
    cursorEj2 = con.cursor()
    num_disp = df_devices['id'].nunique()
    print("El número de dispositivos es: " + str(num_disp))


    print("El número de alertas es: " + str(len(df_alerts)))


    print("La media de puertos abiertos es: " + str(df_analisis['numberports'].mean()))

    print("La desviación estándar del total de puertos abiertos es: " + str(df_analisis['numberports'].std()))

    print("La media de servicios inseguros detectados es: " + str(df_analisis['servicios_inseguros'].mean()))

    print("La desviación estándar del total de servicios inseguros detectados es: " + str(df_analisis['servicios_inseguros'].std()))

    print("La media de vulnerabilidades detectadas es: " + str(df_analisis['vulnerabilidades'].mean()))

    print("La desviación estándar del total de vulnerabilidades detectadas es: " + str(df_analisis['vulnerabilidades'].std()))

    print("El mínimo número de puertos abiertos que se han detectado es: " + str(df_analisis['numberports'].min()))
    print("El máximo número de puertos abiertos que se han detectado es: " + str(df_analisis['numberports'].max()))
    print("El mínimo número de vulnerabilidades que se han detectado es: " + str(df_analisis['vulnerabilidades'].min()))
    print("El máximo número de vulnerabilidades que se han detectado es: " + str(df_analisis['vulnerabilidades'].max()))

def ejercicio3(con):

    cursorEj3 = con.cursor()




    # alertas = cursorEj3.execute('''SELECT  COUNT(*) FROM devices JOIN alerts on alerts.origen = devices.ip JOIN analisis on devices.analisis_id = analisis.id  WHERE alerts.prioridad=1''').fetchone()
    # print("El número de alertas de prioridad 1 es: " + str(alertas[0]))
    #
    # mediana = cursorEj3.execute('''SELECT vulnerabilidades FROM devices JOIN alerts on alerts.origen = devices.ip JOIN analisis on devices.analisis_id = analisis.id  WHERE alerts.prioridad=1''').fetchall()
    # mediana_valor = statistics.median(mediana)
    # print("La mediana de vulnerabilidades: " + str(print(mediana_valor)))

def ejercicio4(con):
    ##ejercicio 4##
    data = pd.read_csv("alerts.csv")

    ##ip_problematicas = data[data["Prioridad de la alerta"] == 1].groupby("IP de origen").size().sort_values(ascending=False)
    ##queda comparar la ip origen con el número de alertas, sacando los datos del csv
    ##una vez tengamos eso, se debería generar el gráfico sin ningún problema
    # Tomar las 10 IP de origen con más alertas
    ips = ip_problematicas.head(10)

    # Crear el gráfico de barras
    plt.bar(ips.index, ips)

    # Añadir título y etiquetas a los ejes
    plt.title("IP de origen más problemáticas")
    plt.xlabel("IP de origen")
    plt.ylabel("Número de alertas con prioridad 1")

    # Rotar etiquetas del eje x para que sean legibles
    plt.xticks(rotation=90)

    # Mostrar el gráfico
    plt.show()


con = sqlite3.connect('devices.db')

cursorObj = con.cursor()

cursorObj.execute('''DROP TABLE IF EXISTS alerts''')
cursorObj.execute('''DROP TABLE IF EXISTS analisis''')
cursorObj.execute('''DROP TABLE IF EXISTS responsable''')
cursorObj.execute('''DROP TABLE IF EXISTS devices''')


cursorObj.execute('''CREATE TABLE IF NOT EXISTS alerts (timestamp timestamp ,sid text ,msg text ,clasificacion text ,prioridad int,protocolo text,origen text ,destino text ,puerto int)''')
cursorObj.execute('''CREATE TABLE IF NOT EXISTS analisis(id INTEGER PRIMARY KEY, puertosabiertos text, numberports int, servicios int, servicios_inseguros int, vulnerabilidades int)''')
cursorObj.execute('''CREATE TABLE IF NOT EXISTS responsable(nombre text PRIMARY KEY, telefono text, rol text)''')
cursorObj.execute('''CREATE TABLE IF NOT EXISTS devices(id text PRIMARY KEY, ip text, localizacion text, nombre_responsable text, analisis_id int, FOREIGN KEY(nombre_responsable) REFERENCES responsable(nombre), FOREIGN KEY(analisis_id) REFERENCES analisis(id))''')


cursorObj.execute('''INSERT INTO responsable VALUES ("admin", "656445552","Administracion de sistemas") ON CONFLICT(nombre) DO NOTHING''')
cursorObj.execute('''INSERT INTO responsable VALUES ("Paco Garcia", "640220120","Direccion") ON CONFLICT(nombre) DO NOTHING''')
cursorObj.execute('''INSERT INTO responsable VALUES ("Luis Sanchez", "None","Desarrollador") ON CONFLICT(nombre) DO NOTHING''')
cursorObj.execute('''INSERT INTO responsable VALUES ("admin", "656445552","Administracion de sistemas") ON CONFLICT(nombre) DO NOTHING''')
cursorObj.execute('''INSERT INTO responsable VALUES ("admiin", "None","None") ON CONFLICT(nombre) DO NOTHING''')
cursorObj.execute('''INSERT INTO responsable VALUES ("admin", "656445552","Administracion de sistemas") ON CONFLICT(nombre) DO NOTHING''')
cursorObj.execute('''INSERT INTO responsable VALUES ("admin", "656445552","Administracion de sistemas") ON CONFLICT(nombre) DO NOTHING''')


cursorObj.execute('''INSERT INTO analisis VALUES(1,"80/TCP, 443/TCP, 3306/TCP, 40000/UDP",4,3,0,15) ON CONFLICT(id) DO NOTHING''')
cursorObj.execute('''INSERT INTO analisis VALUES(2,"None",0,0,0,4) ON CONFLICT(id) DO NOTHING''')
cursorObj.execute('''INSERT INTO analisis VALUES(3,"1194/UDP, 8080/TCP, 8080/UDP, 40000/UDP",4,1,1,52) ON CONFLICT(id) DO NOTHING''')
cursorObj.execute('''INSERT INTO analisis VALUES(4,"443/UDP, 80/TCP",2,1,0,3) ON CONFLICT(id) DO NOTHING''')
cursorObj.execute('''INSERT INTO analisis VALUES(5,"80/TCP, 67/UDP, 68/UDP",3,2,2,12) ON CONFLICT(id) DO NOTHING''')
cursorObj.execute('''INSERT INTO analisis VALUES(6,"8080/TCP, 3306/TCP, 3306/UDP",3,2,0,2) ON CONFLICT(id) DO NOTHING''')
cursorObj.execute('''INSERT INTO analisis VALUES(7,"80/TCP, 443/TCP, 9200/TCP, 9300/TCP, 5601/TCP",5,3,2,21) ON CONFLICT(id) DO NOTHING''')


cursorObj.execute('''INSERT INTO devices VALUES("web","172.18.0.0","None","admin",1)''')
cursorObj.execute('''INSERT INTO devices VALUES("paco_pc","172.17.0.0","Barcelona","Paco Garcia",2)''')
cursorObj.execute('''INSERT INTO devices VALUES("luis_pc","172.19.0.0","Madrid","Luis Sanchez",3)''')
cursorObj.execute('''INSERT INTO devices VALUES("router1","172.1.0.0","None","admin",4)''')
cursorObj.execute('''INSERT INTO devices VALUES("dhcp_server","172.1.0.1","Madrid","admiin",5)''')
cursorObj.execute('''INSERT INTO devices VALUES("mysql_db","172.18.0.1","None","admin",6)''')
cursorObj.execute('''INSERT INTO devices VALUES("ELK","172.18.0.2","None","admin",7)''')



con.commit()



df_alerts = pd.read_csv('alerts.csv')
df_alerts.to_sql('alerts', con, if_exists='replace', index=False)

df_responsable = pd.read_sql_query("SELECT * from responsable", con) #tabla pasada a dataframe
df_analisis = pd.read_sql_query("SELECT * from analisis", con)
df_devices = pd.read_sql_query("SELECT * from devices", con)
df_devices.to_csv('devices.csv',index=False)

df_alljoined = pd.read_sql_query("SELECT * FROM alerts INNER JOIN devices ON alerts.origen = devices.ip OR alerts.destino = devices.ip INNER JOIN analisis ON devices.analisis_id = analisis.id", con)


for i in range(1,4):

    print("---------------- FILTRADO POR PRIORIDAD = " + str(i) + "----------")
    df_prio = df_alljoined.loc[df_alljoined['prioridad'] == i]

    print("El número de observaciones de prioridad " + str(i) +  " es: " + str(len(df_prio)))

    df_none = df_prio.loc[df_prio['localizacion'] == 'None']
    print("El número de campos None es: " + str(len(df_none)))

    print("La mediana del número de vulnerabilidades es: " + str(df_prio['vulnerabilidades'].median()))

    print("La media del número de vulnerabilidades es: " + str(df_prio['vulnerabilidades'].mean()))

    print("La varianza del número de vulnerabilidades es: " + str(df_prio['vulnerabilidades'].var()))

    print("El número mínimo de vulnerabilidades es: " + str(df_prio['vulnerabilidades'].min()))

    print("El número máximo de vulnerabilidades es: " + str(df_prio['vulnerabilidades'].max()))
# Seleccionar las filas donde la columna ip coincide con origen o destino
#df_filtered = merged_df.loc[(merged_df['ip'] == merged_df['origen']) | (merged_df['ip'] == merged_df['destino'])]

# Seleccionar solo la columna vulnerabilidades
#vulnerabilidades = df_filtered['vulnerabilidades']
df_alljoined.to_csv('alljoined.csv')

print(df_alljoined.head(10))

df_alljoined['timestamp'] = pd.to_datetime(df_alljoined['timestamp'])

for i in range(7,9):

    print("----------------- FILTRADO POR MES = " + str(i) + "----------------")

    df_date = df_alljoined.loc[df_alljoined['timestamp'].dt.month == i]
    print(df_date)

    print("El número de observaciones en el mes número  " + str(i) + " es: " + str(len(df_date)))

    df_none = df_date.loc[df_date['localizacion'] == 'None']
    print("El número de campos None es: " + str(len(df_none)))

    print("La mediana del número de vulnerabilidades es: " + str(df_date['vulnerabilidades'].median()))

    print("La media del número de vulnerabilidades es: " + str(df_date['vulnerabilidades'].mean()))

    print("La varianza del número de vulnerabilidades es: " + str(df_date['vulnerabilidades'].var()))

    print("El número mínimo de vulnerabilidades es: " + str(df_date['vulnerabilidades'].min()))

    print("El número máximo de vulnerabilidades es: " + str(df_date['vulnerabilidades'].max()))

#creacionTablas(con)
#alerts = pd.read_csv('alerts.csv')



#print(df_devices.head(20))
#print(df_alerts.head(20))
#alerts.to_sql('alerts', con, if_exists='append', index=False)
ejercicio3(con)







