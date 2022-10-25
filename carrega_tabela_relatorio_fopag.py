import fdb, csv
import psycopg2

con_pg = psycopg2.connect(host="localhost",database="relatorio_fopag",user="postgres",password="esus", port=5433)
con_fdb = fdb.connect(dsn='localhost:/Fiorilli/SIP_DADOS/SIP.FDB', user='fscsip', password='sip')
cur_fdb = con_fdb.cursor()
cur_pg = con_pg.cursor()


def nome_errados():
    sql = """
    SELECT T.REGISTRO, T.NOME, P.NOME 
    FROM TRABALHADOR AS T
    INNER JOIN PESSOA AS P ON P.CPF = T.CPF
    WHERE T.SITUACAO = 1
    ORDER BY T.REGISTRO
    """
    cur_fdb.execute(sql)
    consulta = cur_fdb.fetchall()
    lista = []
    for linha in consulta:
        if linha[1] != linha[2]:
            lista.append(linha)
    
    with open("nomes_errados_apos_att_oficial.csv", 'w',newline='',encoding='utf-8') as f:
        write = csv.writer(f, delimiter='₢') #delimitador ₢ (AltGr + C)
        write.writerows(lista)
    
    return lista

def atualiza_nomes_errados(lista):
    for linha in lista:
        nome = linha[2]
        registro = linha[0]
        update_db = f"UPDATE TRABALHADOR SET NOME = '{nome}' WHERE REGISTRO = {registro}"
        cur_fdb.execute(update_db)
    con_fdb.commit()
    
        
def carga_divisao_relatorio(evento,ano,mes):
    for divisao in range(1,18):
        divisao = "{:02d}".format(divisao)
        sql = f"""
        SELECT SUM(M.VALOR), COUNT(M.CODIGO) 
        FROM MOVIMENTO AS M 
        INNER JOIN REFERENCIA AS R ON M.REFERENCIA = R.CODIGO 
        INNER JOIN EVENTOS AS E ON M.EVENTO = E.CODIGO
        INNER JOIN TRABALHADOR AS T ON M.REGISTRO = T.REGISTRO
        WHERE EVENTO = {evento} AND ANO = {ano} AND MES = {mes} AND T.DIVISAO = {divisao}
        """
        cur_fdb.execute(sql)
        consulta = cur_fdb.fetchone()
       
        if consulta is None:
            total_divisao = '0'
            quantidade_divisao = '0'
        else:
            total_divisao = "{:_.2f}".format(round(consulta[0],2)).replace('.',',').replace('_','.') if consulta[0] is not None else '0'
            quantidade_divisao = "{:_.0f}".format(consulta[1]).replace('.',',').replace('_','.') if consulta[0] is not None else '0'
            
        update_db = f"""
        UPDATE RELATORIO
        SET QUANTIDADE_DIVISAO_{divisao} = %s, TOTAL_DIVISAO_{divisao} = %s
        WHERE EVENTO = %s AND ANO = %s AND MES = %s"""
        cur_pg.execute(update_db,(quantidade_divisao,total_divisao,evento,ano,mes))
        con_pg.commit()
    
def update_natureza(evento,ano,mes):
    cur_fdb.execute(f"""SELECT NATUREZA FROM EVENTOS WHERE CODIGO = '{evento}' """)
    natureza = cur_fdb.fetchone()[0]
    
    sql =f"""
    UPDATE RELATORIO
    SET NATUREZA = %s
    WHERE EVENTO = %s AND ANO = %s AND MES = %s"""
    
    cur_pg.execute(sql,(natureza,evento,ano,mes))
    con_pg.commit()
    
def carga_tabela_relatorio(folha,evento,ano,mes):
    sql = f"""
    SELECT E.NOME, SUM(M.VALOR), COUNT(M.CODIGO) 
    FROM MOVIMENTO AS M 
    INNER JOIN REFERENCIA AS R ON M.REFERENCIA = R.CODIGO 
    INNER JOIN EVENTOS AS E ON M.EVENTO = E.CODIGO 
    WHERE M.EVENTO = {evento} AND R.ANO = {ano} AND R.MES = {mes} AND R.TIPO = {folha}
    GROUP BY E.NOME
    """
    
    cur_fdb.execute(sql)
    consulta = cur_fdb.fetchone()
    if consulta is None:
        cur_fdb.execute(f"""SELECT NOME FROM EVENTOS WHERE CODIGO = '{evento}' """)
        nome_evento = cur_fdb.fetchone()[0]
        total_evento = '0'
        quantidade_evento = '0'
    else:
        nome_evento = consulta[0]
        total_evento = consulta[1]
        quantidade_evento = consulta[2]

    cur_pg.execute(f"""SELECT COUNT(*) FROM RELATORIO WHERE EVENTO = '{evento}' AND ANO = '{ano}' AND MES = '{mes}'""")
    consulta = cur_pg.fetchone()[0]
    
    if consulta == 0:
        insert_db = """
        INSERT INTO RELATORIO 
        (EVENTO,ANO,MES,NOME,QUANTIDADE_GERAL_EVENTO,TOTAL_GERAL_EVENTO) 
        VALUES (%s,%s,%s,%s,%s,%s)"""
        cur_pg.execute(insert_db,(evento,ano,mes,nome_evento,quantidade_evento,total_evento))
        con_pg.commit()
        carga_divisao_relatorio(evento,ano,mes)
        update_natureza(evento,ano,mes)
        
    else:
        update_db = """
        UPDATE RELATORIO
        SET NOME = %s,QUANTIDADE_GERAL_EVENTO = %s,TOTAL_GERAL_EVENTO = %s
        WHERE EVENTO = %s AND ANO = %s AND MES = %s"""
        cur_pg.execute(update_db,(nome_evento,quantidade_evento,total_evento,evento,ano,mes))
        con_pg.commit()
        carga_divisao_relatorio(evento,ano,mes)
        update_natureza(evento,ano,mes)
    
if __name__ == "__main__":
    
    # ano = input("Ano (yyyy): ")
    # folha = input("Folha: ")
    # print("mes (mm): ")
    # mes = input()
    print("Aguarde...carregando no banco de dados!")
    
    lista = nome_errados()
    # atualiza_nomes_errados(lista)
    
    # for mes in range(10,13):
    #     mes = "{:02d}".format(mes)
    #     for evento in range(1,1000):
    #         evento = "{:03d}".format(evento)
    #         try:
    #             carga_tabela_relatorio(folha,evento,ano,mes)
    #         except TypeError:
    #             pass
            
    
    cur_fdb.close()
    cur_pg.close()
    con_fdb.close()
    con_pg.close()