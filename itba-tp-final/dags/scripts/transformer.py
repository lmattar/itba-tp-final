#!/usr/bin/env python3.6

# la linea de arriba es le shebang que indica que interprete de python se usa para ejecutar el script
# y para que funcionen los modulos instalados en airflow hay que usar el python3.6 (eso lo deduje del error que tiraba)
# es que el que usa ariflow 
from datetime import datetime
import sys
import socket
from pprint import  pprint
import pandas as pd
import numpy as np
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
# prom = df[['FL_DATE','ORIGIN','DEP_DELAY']].groupby(['ORIGIN','FL_DATE']).mean()


# #TODO dropnan

# with open(f'/opt/airflow/dags/sql/inserts_{year}.sql','w'):
#         for index, row in prom.iterrows():
#             print(row)
#             print(index)
#             print('INSERT INTO dep_delay_mean(' + 
#                             'ORIGIN' + 
#                             ',FL_DATE' +
#                             ',DEP_DELAY_MEAN' +
#                             ')' +
#                             'VALUES(' + '\'' +index[0]+ '\'' +
#                             ',\'' +index[1]+ '\'' +
#                             ',' +str(row['DEP_DELAY'])+ 
#                             ');',file=open(f'/opt/airflow/dags/sql/inserts_{year}.sql','a')) 



#------------------------------------------------------------
#-----------------------outliers----------------------------
#------------------------------------------------------------
df['DEP_DELAY'] = df['DEP_DELAY'].astype(float)
df_grp =  df[['FL_DATE','ORIGIN','DEP_DELAY']].groupby(['ORIGIN','FL_DATE']).mean() #df[['FL_DATE','ORIGIN']].groupby(['ORIGIN','FL_DATE']).mean()

np.seterr(divide='ignore', invalid='ignore')
def detect_outlier(data_1):
    outliers=[]
    threshold=3
    mean_1 = np.mean(data_1)
    std_1 =np.std(data_1)
    print(f"standardd:{std_1}")
    
    for y in data_1:
       # print(y)
        z_score= (y - mean_1)/std_1 
        if np.abs(z_score) > threshold:
            outliers.append(y)
    #print(outliers)   
    return outliers


with open(f'/opt/airflow/dags/sql/update_outliers_{year}.sql','w'):
  for row_label, row in df_grp.iterrows():
      print(row_label[0])
      print(row_label[1])
      df_f = df[df["ORIGIN"]==row_label[0]]
      print(df.head(10))
      df1 = df_f[["DEP_DELAY"]]
      #print(type(np.array(df_f)))
      #print(f"outliers--> ORIGIN: {row_label[0]} DATE : {row_label[1]}")
      ot = detect_outlier(np.array(df1))
      print(ot)
      if len(ot)>0:
        print(f"UPDATE flights_per_day SET outliers = {len(ot)} WHERE ORIGIN=\"{row_label[0]}\" AND FL_DATE=\"{row_label[1]}\";",file=open(f'/opt/airflow/dags/sql/update_outliers_{year}.sql','a'))
        #print(s)    




prom.to_csv(output)



print("Completed data transformation!")