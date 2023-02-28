import json
import sqlite3
import pandas as pd





def funcionPrincipal(con):
    cursorObj = con.cursor()
    cursorObj.execute('''CREATE TABLE alerts (timestampa text ,sid text ,msg text ,clasificacion text ,prioridad int,protocolo text,origen text ,destino text ,puerto int)''')
    cursorObj.execute('''CREATE TABLE devices(id text PRIMARY KEY, ip text, localizacion text, ''')
    cursorObj.execute('''CREATE TABLE responsable(id text PRIMARY KEY, nombre text, telefono text, rol text)''')
    cursorObj.execute('''CREATE TABLE analisis(id INTEGER PRIMARY KEY, puertosabiertos text, ''' )
    con.commit()




def sql_count_alerts(con):
    cursorObj = con.cursor()
    cursorObj.execute('SELECT COUNT(*) from alerts')
    num = cursorObj.fetchone()
    print("El número de alertas es: " + str(num[0]))

con = sqlite3.connect('example.db')

sql_create_table(con)
df = pd.read_json('devices.json')
dev = sqlite3.connect('devices.db')


alerts = pd.read_csv('alerts.csv')
alerts.to_sql('alerts', con, if_exists='append', index=False)

f = open('devices.json','r')
devices = json.load(f)



sql_count_distinct(con)
sql_count_alerts(con)
