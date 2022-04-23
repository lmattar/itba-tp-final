#!/usr/bin/env python3.6

# la linea de arriba es le shebang que indica que interprete de python se usa para ejecutar el script
# y para que funcionen los modulos instalados en airflow hay que usar el python3.6 (eso lo deduje del error que tiraba)
# es que el que usa ariflow 
from datetime import datetime
import sys
import socket
from pprint import  pprint
import pandas as pd
input=sys.argv[1]
output=sys.argv[2]

print("Starting data transformation...")
# print(input)
# print(f'tipos {type(input)}')
# f = open(input, "r")
# print(f.read())
# pprint(sys.path)
df = pd.read_csv(input)
date = datetime.strptime(df.iloc[0]['FL_DATE'],'%Y-%m-%d')
year  = str(date.year)
# df.head(100).to_csv(output)
#print('Hostname',socket.gethostname())
prom = df[['FL_DATE','ORIGIN','DEP_DELAY']].groupby(['ORIGIN','FL_DATE']).count()
prom.rename(columns = {'DEP_DELAY':'FL_COUNT'}, inplace = True)

#TODO dropnan
with open(f'/opt/airflow/dags/sql/inserts_per_day_{year}.sql','w'):
        for index, row in prom.iterrows():
            print(row)
            print(index)
            print('INSERT INTO flights_per_day(' + 
                            'ORIGIN' + 
                            ',FL_DATE' +
                            ',FL_COUNT' +
                            ')' +
                            'VALUES(' + '\'' +index[0]+ '\'' +
                            ',\'' +index[1]+ '\'' +
                            ',' +str(row['FL_COUNT'])+ 
                            ');',file=open(f'/opt/airflow/dags/sql/inserts_per_day_{year}.sql','a')) 


prom.to_csv(output)



print("Completed data transformation!")