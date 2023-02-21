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
    #SELECT dni, nombre FROM usuarios WHERE altura > 1.0
    rows = cursorObj.fetchall()
    for row in rows:
        print(row)
#
def sql_delete_table(con):
    cursorObj = con.cursor()
    cursorObj.execute('DROP TABLE alerts')
    con.commit()
#
# def sql_delete_table(con):
#     cursorObj = con.cursor()
#     cursorObj.execute('drop table if exists usuarios')
#     con.commit()

def sql_create_table(con):
    cursorObj = con.cursor()
    cursorObj.execute('''CREATE TABLE alerts (timestampa text ,sid text ,msg text ,clasificacion text ,prioridad int,protocolo text,origen text ,destino text ,puerto int)''')
    con.commit()

con = sqlite3.connect('example.db')
sql_delete_table(con)
sql_create_table(con)

alerts = pd.read_csv('alerts.csv')
alerts.to_sql('alerts', con, if_exists='append', index=False)


sql_fetch(con)
# sql_update(con)
# sql_fetch(con)

# sql_fetch(con)
# sql_delete_table(con)
# con.close()