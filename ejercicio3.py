import sqlite3
import pandas as pd

# def sql_update(con):
#     cursorObj = con.cursor()
#     cursorObj.execute('UPDATE usuarios SET nombre = "Sergio" where dni = "X"')
#     con.commit()
#
def sql_fetch(con):
    cursorObj = con.cursor()
    cursorObj.execute('SELECT * FROM alerts')
    rows = cursorObj.fetchall()
    for row in rows:
        print(row)
#
def sql_delete_table(con):
    cursorObj = con.cursor()
    cursorObj.execute('DROP TABLE alerts')
    con.commit()


def sql_create_table(con):
    cursorObj = con.cursor()
    cursorObj.execute('''CREATE TABLE alerts (timestampa text ,sid text ,msg text ,clasificacion text ,prioridad int,protocolo text,origen text ,destino text ,puerto int)''')
    con.commit()


def sql_count_distinct(con):
    cursorObj = con.cursor()
    cursorObj.execute('SELECT COUNT(DISTINCT sid) from alerts')
    num = cursorObj.fetchone()
    print("El número distinto de dispositivos es: " + str(num[0]))

def sql_count_alerts(con):
    cursorObj = con.cursor()
    cursorObj.execute('SELECT COUNT(msg) from alerts')
    num = cursorObj.fetchone()
    print("El número de alertas es: " + str(num[0]))

con = sqlite3.connect('example.db')
sql_delete_table(con)
sql_create_table(con)

alerts = pd.read_csv('alerts.csv')
alerts.to_sql('alerts', con, if_exists='append', index=False)


#sql_fetch(con)
sql_count_distinct(con)
sql_count_alerts(con)
