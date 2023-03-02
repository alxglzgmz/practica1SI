import json
import sqlite3
import pandas as pd





def creacionTablas(con):
    cursorObj = con.cursor()

    cursorObj.execute('''DROP TABLE IF EXISTS alerts''')
    cursorObj.execute('''DROP TABLE IF EXISTS analisis''')
    cursorObj.execute('''DROP TABLE IF EXISTS responsable''')
    cursorObj.execute('''DROP TABLE IF EXISTS devices''')


    cursorObj.execute('''CREATE TABLE IF NOT EXISTS alerts (timestampa text ,sid text ,msg text ,clasificacion text ,prioridad int,protocolo text,origen text ,destino text ,puerto int)''')
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
    # rowsResponsable = cursorObj.execute('''SELECT * FROM responsable''').fetchall()
    # for rows in rowsResponsable:
    #     print(rows)

    cursorObj.execute('''INSERT INTO analisis VALUES(1,"80/TCP, 443/TCP, 3306/TCP, 40000/UDP",4,3,0,15) ON CONFLICT(id) DO NOTHING''')
    cursorObj.execute('''INSERT INTO analisis VALUES(2,"None",0,0,0,4) ON CONFLICT(id) DO NOTHING''')
    cursorObj.execute('''INSERT INTO analisis VALUES(3,"1194/UDP, 8080/TCP, 8080/UDP, 40000/UDP",4,1,1,52) ON CONFLICT(id) DO NOTHING''')
    cursorObj.execute('''INSERT INTO analisis VALUES(4,"443/UDP, 80/TCP",2,1,0,3) ON CONFLICT(id) DO NOTHING''')
    cursorObj.execute('''INSERT INTO analisis VALUES(5,"80/TCP, 67/UDP, 68/UDP",3,2,2,12) ON CONFLICT(id) DO NOTHING''')
    cursorObj.execute('''INSERT INTO analisis VALUES(6,"8080/TCP, 3306/TCP, 3306/UDP",3,2,0,2) ON CONFLICT(id) DO NOTHING''')
    cursorObj.execute('''INSERT INTO analisis VALUES(7,"80/TCP, 443/TCP, 9200/TCP, 9300/TCP, 5601/TCP",5,3,2,21) ON CONFLICT(id) DO NOTHING''')
    # rowsAnalisis = cursorObj.execute('''SELECT * FROM analisis''').fetchall()
    # for rows in rowsAnalisis:
    #     print(rows)


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
    numDisp = cursorEj2.execute('''SELECT COUNT (DISTINCT id) from devices''').fetchone()
    print("El número de dispositivos es: " + str(numDisp[0]))
    numAlertas = cursorEj2.execute('''SELECT COUNT(*) from alerts''').fetchone()
    print("El número de alertas es: " + str(numAlertas[0]))

    mediaPuertos = cursorEj2.execute('''SELECT AVG(numberports) from analisis''').fetchone()
    print("La media de puertos abiertos es: " + str(mediaPuertos[0]))
    media = mediaPuertos[0]
    desvPuertos = cursorEj2.execute('''SELECT SQRT(SUM(POWER((numberports - media), 2)) / COUNT(numberports)) as desviacion_estandar
FROM analisis
CROSS JOIN (SELECT AVG(numberports) as media FROM analisis) as subconsulta''')
    print("La desviacion tipica de puertos abiertos es: " + str(desvPuertos[0]))








def sql_count_alerts(con):
    cursorObj = con.cursor()
    cursorObj.execute('SELECT COUNT(*) from alerts')
    num = cursorObj.fetchone()
    print("El número de alertas es: " + str(num[0]))

con = sqlite3.connect('devices.db')


creacionTablas(con)

alerts = pd.read_csv('alerts.csv')
alerts.to_sql('alerts', con, if_exists='append', index=False)
ejercicio2(con)




f = open('devices.json','r')
devices = json.load(f)




