import csv, fdb
from datetime import datetime
con = fdb.connect(dsn='192.168.2.140:/Fiorilli/SIP_DADOS/SIP.FDB', user='fscsip', password='sip')
cur = con.cursor()

erros = []
corretos = []
with open('junho_2022.csv','r') as f:
    reader = csv.reader(f,delimiter=';',quoting=csv.QUOTE_ALL)
    next(reader)
    for linha in reader:
        registro = linha[0]
        nome = linha[1]
        evento = linha[2]
        horas = linha[4][:2]
        horas = horas.strip()
        situacao = linha[5]
        
        valor = float(horas) * 20     #valor da hora é 20 reais
        
        sql_nome = f"""SELECT NOME FROM TRABALHADOR WHERE REGISTRO = {registro} AND SITUACAO = 1"""
        cur.execute(sql_nome)
        consulta_nome = cur.fetchone()
        if consulta_nome is not None:
            if consulta_nome[0] == nome:
                
                movimentos = f"""SELECT EVENTO,VALOR FROM MOVIMENTO AS M
                INNER JOIN REFERENCIA AS R ON R.CODIGO = M.REFERENCIA 
                WHERE R.ANO = 2022 AND MES = 06 AND R.TIPO = 1 AND M.REGISTRO = {registro} AND M.EVENTO = {evento}"""
                cur.execute(movimentos)
                movimento = cur.fetchone()
                if movimento is not None:
                    if valor == movimento[1]:
                        corretos.append({'valor correto':{registro:nome}})
                    else:
                        erros.append({'valor incorreto':{registro:nome}})
                elif movimento is None and situacao == 'CANCELAMENTO':
                    corretos.append({'evento removido correto':{registro:nome}})
                else:
                    erros.append({'evento nao incluido':{registro:nome}})
            else:
                erros.append({'registro/nome':{registro:nome}})
        else:
            erros.append({'registro/nome':{registro:nome}})

print(erros)

