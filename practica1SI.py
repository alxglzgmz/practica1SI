import json
import sqlite3
import pandas as pd
import statistics
import matplotlib.pyplot as plt

def ejercicio2():
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

def ejercicio3():
    for i in range(1, 4):
        print("---------------- FILTRADO POR PRIORIDAD = " + str(i) + "----------")
        df_prio = df_alljoined.loc[df_alljoined['prioridad'] == i]

        print("El número de observaciones de prioridad " + str(i) + " es: " + str(len(df_prio)))

        df_none = df_prio.loc[df_prio['localizacion'] == 'None']
        print("El número de campos None es: " + str(len(df_none)))

        print("La mediana del número de vulnerabilidades es: " + str(df_prio['vulnerabilidades'].median()))

        print("La media del número de vulnerabilidades es: " + str(df_prio['vulnerabilidades'].mean()))

        print("La varianza del número de vulnerabilidades es: " + str(df_prio['vulnerabilidades'].var()))

        print("El número mínimo de vulnerabilidades es: " + str(df_prio['vulnerabilidades'].min()))

        print("El número máximo de vulnerabilidades es: " + str(df_prio['vulnerabilidades'].max()))

    df_alljoined['timestamp'] = pd.to_datetime(df_alljoined['timestamp'])

    for i in range(7, 9):
        print("----------------- FILTRADO POR MES = " + str(i) + "----------------")

        df_date = df_alljoined.loc[df_alljoined['timestamp'].dt.month == i]

        print("El número de observaciones en el mes número  " + str(i) + " es: " + str(len(df_date)))

        df_none = df_date.loc[df_date['localizacion'] == 'None']
        print("El número de campos None es: " + str(len(df_none)))

        print("La mediana del número de vulnerabilidades es: " + str(df_date['vulnerabilidades'].median()))

        print("La media del número de vulnerabilidades es: " + str(df_date['vulnerabilidades'].mean()))

        print("La varianza del número de vulnerabilidades es: " + str(df_date['vulnerabilidades'].var()))

        print("El número mínimo de vulnerabilidades es: " + str(df_date['vulnerabilidades'].min()))

        print("El número máximo de vulnerabilidades es: " + str(df_date['vulnerabilidades'].max()))

def ejercicio4():
    """""
    ##ejercicio 4.1##
    df_alerts_p1 = df_alerts.query('prioridad == 1')
    df_ip_alertas = df_alerts_p1.groupby('origen')['prioridad'].count()
    df_ip_alertas = df_ip_alertas.sort_values(ascending=False)
    top_10_ip = df_ip_alertas.head(10)
    plt.bar(top_10_ip.index, top_10_ip.values)
    plt.xlabel('IP de origen')
    plt.ylabel('Número de alertas')
    plt.title('10 IP de origen más problemáticas')
    plt.show()
    """
    """""
    ###ejercicio4.2###
    cursorEj3 = con.cursor()
    alertas = cursorEj3.execute('''SELECT  COUNT(*) FROM devices JOIN alerts on alerts.origen = devices.ip JOIN analisis on devices.analisis_id = analisis.id  WHERE alerts.prioridad=1''').fetchone()
    df_alerts['timestamp'] = pd.to_datetime(df_alerts['timestamp'])
    df_alerts = df_alerts.set_index('timestamp')
    alertas_por_dia = df_alerts.resample('D')['alertas'].count()
    alertas_por_dia.plot(figsize=(12, 6))
    plt.xlabel('Fecha')
    plt.ylabel('Número de alertas')
    plt.title('Número de alertas en el tiempo')
    plt.show()
    """
    ###ejercicio 4.3###
    alertas_por_categoria = df_alerts.groupby('prioridad')['sid'].count()
    alertas_por_categoria = alertas_por_categoria.sort_values(ascending=False)
    plt.bar(alertas_por_categoria.index, alertas_por_categoria.values)
    plt.xlabel('Categoría')
    plt.ylabel('Número de alertas')
    plt.title('Número de alertas por categoría')
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
df_responsable = pd.read_sql_query("SELECT * from responsable", con)
df_analisis = pd.read_sql_query("SELECT * from analisis", con)
df_devices = pd.read_sql_query("SELECT * from devices", con)
df_devices.to_csv('devices.csv',index=False)

df_alljoined = pd.read_sql_query("SELECT * FROM alerts INNER JOIN devices ON alerts.origen = devices.ip OR alerts.destino = devices.ip INNER JOIN analisis ON devices.analisis_id = analisis.id", con)



ejercicio3()






