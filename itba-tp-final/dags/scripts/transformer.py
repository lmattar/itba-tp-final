#!/usr/bin/python3


import sys
#import pandas as pd
input=sys.argv[1]
output=sys.argv[2]

print("Starting data transformation...")
print(input)
print(f'tipos {type(input)}')
f = open(input, "r")
print(f.read())
#df = pd.read_csv(input)
#df.head(10)
print("Completed data transformation!")