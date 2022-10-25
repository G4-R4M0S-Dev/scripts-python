from tkinter.tix import Tree
import pandas as pd
import csv


list_dir, list_rec = [],[]

with open('direito_alimento.csv','r',encoding='utf-8') as f:
    dir = csv.reader(f, delimiter=',', quoting=csv.QUOTE_ALL)
    next(dir)
    for linha in dir:
        list_dir.append(linha)
        
        
with open('receberam_alimento.csv','r',encoding='utf-8') as f:
    rec = csv.reader(f, delimiter=',', quoting=csv.QUOTE_ALL)
    next(rec)
    for linha in rec:
        list_rec.append(linha)

with open('nao_receberam_aux_alimentacao_maio.csv','w',encoding='utf-8', newline='') as f:
    rec = csv.writer(f, delimiter=',', quoting=csv.QUOTE_ALL)
    for item in list_dir:
        if not item in list_rec:
            rec.writerow(item)
        



