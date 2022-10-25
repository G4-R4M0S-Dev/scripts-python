# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

from dash import Dash, html, dcc
import plotly.express as px
import pandas as pd
pd.options.mode.chained_assignment = None
from sqlalchemy import create_engine
import os

engine = create_engine('firebird+fdb://fscsip:sip@localhost:3050/C:/Fiorilli/SIP_DADOS/SIP.FDB')
# engine = create_engine('firebird+fdb://fscsip:sip@192.168.2.140:3050/C:/Fiorilli/SIP_DADOS/SIP.FDB')
con = engine.connect()

def movimentos(folha,ano,mes):    
    sql_movimentos = f"""
        SELECT M.REGISTRO, M.REFERENCIA, M.EVENTO,E.NOME,E.NATUREZA, M.VALOR, R.ANO,R.MES,R.TIPO,R.DTPAGTO,R.DTFECHA,R.ULTIMO_DIA,CLASSIFICACAO_ESOCIAL AS COD_ESOCIAL
        FROM MOVIMENTO AS M 
        INNER JOIN REFERENCIA AS R ON M.REFERENCIA = R.CODIGO
        INNER JOIN EVENTOS AS E ON E.CODIGO = M.EVENTO
        WHERE R.ANO = {ano} AND R.MES = {mes} AND R.TIPO = {folha}
        """
    sql_trabalhador = """
        SELECT REGISTRO, NOME AS P_NOME, CPF, CARGOATUAL AS COD_CARGO, DIVISAO AS COD_DIVISAO, SUBDIVISAO AS COD_SUBDIVISAO, LOCAL_TRABALHO AS COD_LOCALTRAB, 
        VINCULO AS COD_VINCULO,SITUACAO,DTSITUACAO,DTDEMISSAO,DTADMISSAO,DTNASCIMENTO,SEXO,INSTRUCAO AS COD_INSTRUCAO,ESTADOCIVIL AS COD_ESTADOCIVIL
        FROM TRABALHADOR
        """
    sql_contas = """SELECT REGISTRO,CONTA,DVCONTA,AGENCIA,DVAGENCIA FROM CONTASTRABALHADOR WHERE PADRAO = 'S' """
    #--------------------------CARREGANDO NOMES-------------------------#
    sql_tabela_divisao = """SELECT CODIGO,NOME AS DIVISAO FROM DIVISAO """
    sql_tabela_subdivisao = """SELECT CODIGO,NOME AS SUBDIVISAO FROM SUBDIVISAO """
    sql_tabela_local_trabalho = """SELECT CODIGO,NOME AS LOCAL_TRABALHO FROM LOCAL_TRABALHO"""
    sql_tabela_vinculo = """SELECT CODIGO, NOME AS VINCULO FROM VINCULO"""
    sql_tabela_esocial = """SELECT CODIGO,NOME AS ESOCIAL FROM ESOCIAL_CLASS_RUBRICA"""
    sql_tabela_cargo = """SELECT CODIGO,NOME AS CARGO, CBO FROM CARGOS"""
    sql_tabela_instrucao = """SELECT CODIGO,NOME AS INSTRUCAO FROM INSTRUCAO"""
    sql_tabela_estadocivil = """SELECT CODIGO,NOME AS ESTADOCIVIL FROM ESTADOCIVIL"""
    #------------------------------CARREGANDO DADOS HISTORICOS-------------------------#    
    sql_lotacao_historico = """SELECT REGISTRO, DIVISAO AS COD_DIVISAO, ITEM, DHTRANSF, ANTERIOR FROM HISTTRAB_DIVISAO"""
    sql_sublotacao_historico = """SELECT REGISTRO, SUBDIVISAO AS COD_SUBDIVISAO, ITEM, DHTRANSF, ANTERIOR FROM HISTTRAB_SUBDIVISAO"""
    sql_localtrab_historico = """SELECT REGISTRO, LOCALTRAB AS COD_LOCALTRAB, ITEM, DHTRANSF, ANTERIOR FROM HISTTRAB_LOCALTRAB"""
    sql_cargo_historico = """SELECT REGISTRO, CARGO AS COD_CARGO, ITEM, DHTRANSF, ANTERIOR FROM HISTTRAB_CARGO"""
    
    with engine.connect() as con:
        movimentos = pd.read_sql_query(sql_movimentos,con)
        tabela_divisao = pd.read_sql_query(sql_tabela_divisao,con)
        tabela_subdivisao = pd.read_sql_query(sql_tabela_subdivisao,con)
        tabela_local_trabalho = pd.read_sql_query(sql_tabela_local_trabalho,con)
        tabela_vinculo = pd.read_sql_query(sql_tabela_vinculo,con)
        tabela_esocial = pd.read_sql_query(sql_tabela_esocial,con)
        tabela_cargo = pd.read_sql_query(sql_tabela_cargo,con)
        tabela_instrucao = pd.read_sql_query(sql_tabela_instrucao,con)
        tabela_estadocivil = pd.read_sql_query(sql_tabela_estadocivil,con)
        trabalhador = pd.read_sql_query(sql_trabalhador,con)
        lotacao_historico = pd.read_sql_query(sql_lotacao_historico,con)
        sublotacao_historico = pd.read_sql_query(sql_sublotacao_historico,con)
        localtrab_historico = pd.read_sql_query(sql_localtrab_historico,con)   
        cargo_historico = pd.read_sql_query(sql_cargo_historico,con)    
        contas = pd.read_sql_query(sql_contas,con)    
        
    movimentos['dtpagto'] = pd.to_datetime(movimentos['dtpagto'])
    movimentos['ultimo_dia'] = pd.to_datetime(movimentos['ultimo_dia'])
    movimentos['dtfecha'] = pd.to_datetime(movimentos['dtfecha'])
    movimentos['registro'] = pd.to_numeric(movimentos['registro'])
    trabalhador['registro'] = pd.to_numeric(trabalhador['registro'])

    #DATA PARA REFERENCIA DE DADOS HISTORICOS
    data = movimentos['ultimo_dia'][0]
    # if folha == 9:
    #     data = movimentos['ultimo_dia'][0]
    # else:
    #     data = movimentos['dtfecha'][0]
    #--------------------------------CARREGA DIVISAO------------------------------------------------#
    lotacao_historico['registro'] = pd.to_numeric(lotacao_historico['registro'])
    lotacao_historico_registros = lotacao_historico['registro'].drop_duplicates()
    movimentos_copy = pd.merge(movimentos,lotacao_historico_registros,on='registro', indicator='merge', how='left')
    movimentos_lotacao = pd.merge(movimentos_copy,trabalhador,on='registro', how='left').query('merge == "left_only"')
    selecao = lotacao_historico['dhtransf'] <= data
    lotacao_historico_proc_antetior = lotacao_historico[selecao].drop_duplicates(subset=['registro'], keep='last')
    lotacao_historico_proc_posterior = lotacao_historico[~lotacao_historico['registro'].isin(lotacao_historico_proc_antetior['registro'])].drop_duplicates(subset='registro', keep='first')
    lotacao_historico_proc_posterior['cod_divisao'] = lotacao_historico_proc_posterior['anterior']
    lotacao_historico_proc = pd.concat([lotacao_historico_proc_antetior,lotacao_historico_proc_posterior])
    lotacao_historico_proc.reset_index(drop=True, inplace=True)
    lotacao_historico_proc = lotacao_historico_proc[['registro','cod_divisao']]
    movimentos_lotacao_historico = pd.merge(movimentos_copy,lotacao_historico_proc,on='registro', how='left').query('merge == "both"')
    movimentos_proc = pd.concat([movimentos_lotacao_historico,movimentos_lotacao])
    movimentos_proc.reset_index(drop=True, inplace=True)

    #--------------------------------CARREGA SUBDIVISAO------------------------------------------------#
    sublotacao_historico['registro'] = pd.to_numeric(sublotacao_historico['registro'])
    sublotacao_historico_registros = sublotacao_historico['registro'].drop_duplicates()
    movimentos_subcopy = pd.merge(movimentos,sublotacao_historico_registros,on='registro', indicator='submerge', how='left')
    movimentos_sublotacao = pd.merge(movimentos_subcopy,trabalhador,on='registro', how='left').query('submerge == "left_only"')
    selecao = sublotacao_historico['dhtransf'] <= data
    sublotacao_historico_proc_antetior = sublotacao_historico[selecao].drop_duplicates(subset=['registro'], keep='last')
    sublotacao_historico_proc_posterior = sublotacao_historico[~sublotacao_historico['registro'].isin(sublotacao_historico_proc_antetior['registro'])].drop_duplicates(subset='registro', keep='first')
    sublotacao_historico_proc_posterior['cod_subdivisao'] = sublotacao_historico_proc_posterior['anterior']
    sublotacao_historico_proc = pd.concat([sublotacao_historico_proc_antetior,sublotacao_historico_proc_posterior])
    sublotacao_historico_proc.reset_index(drop=True, inplace=True)
    sublotacao_historico_proc = sublotacao_historico_proc[['registro','cod_subdivisao']]
    movimentos_sublotacao_historico = pd.merge(movimentos_subcopy,sublotacao_historico_proc,on='registro', how='left').query('submerge == "both"')
    movimentos_proc_sub = pd.concat([movimentos_sublotacao_historico,movimentos_sublotacao])
    movimentos_proc_sub.reset_index(drop=True, inplace=True)

    #--------------------------------CARREGA LOCAL DE TRABALHO------------------------------------------------#
    localtrab_historico['registro'] = pd.to_numeric(localtrab_historico['registro'])
    localtrab_historico_registros = localtrab_historico['registro'].drop_duplicates()
    movimentos_trab_copy = pd.merge(movimentos,localtrab_historico_registros,on='registro', indicator='trabmerge', how='left')
    movimentos_localtrab = pd.merge(movimentos_trab_copy,trabalhador,on='registro', how='left').query('trabmerge == "left_only"')
    selecao = localtrab_historico['dhtransf'] <= data
    localtrab_historico_proc_antetior = localtrab_historico[selecao].drop_duplicates(subset=['registro'], keep='last')
    localtrab_historico_proc_posterior = localtrab_historico[~localtrab_historico['registro'].isin(localtrab_historico_proc_antetior['registro'])].drop_duplicates(subset='registro', keep='first')
    localtrab_historico_proc_posterior['cod_localtrab'] = localtrab_historico_proc_posterior['anterior']
    localtrab_historico_proc = pd.concat([localtrab_historico_proc_antetior,localtrab_historico_proc_posterior])
    localtrab_historico_proc.reset_index(drop=True, inplace=True)
    localtrab_historico_proc = localtrab_historico_proc[['registro','cod_localtrab']]
    movimentos_localtrab_historico = pd.merge(movimentos_trab_copy,localtrab_historico_proc,on='registro', how='left').query('trabmerge == "both"')
    movimentos_proc_localtrab = pd.concat([movimentos_localtrab_historico,movimentos_localtrab])
    movimentos_proc_localtrab.reset_index(drop=True, inplace=True)

    #--------------------------------CARREGA CARGO------------------------------------------------#
    cargo_historico['registro'] = pd.to_numeric(cargo_historico['registro'])
    cargo_historico_registros = cargo_historico['registro'].drop_duplicates()
    movimentos_cargo_copy = pd.merge(movimentos,cargo_historico_registros,on='registro', indicator='cargo_merge', how='left')
    movimentos_cargo = pd.merge(movimentos_cargo_copy,trabalhador,on='registro', how='left').query('cargo_merge == "left_only"')
    selecao = cargo_historico['dhtransf'] <= data
    cargo_historico_proc_antetior = cargo_historico[selecao].drop_duplicates(subset=['registro'], keep='last')
    cargo_historico_proc_posterior = cargo_historico[~cargo_historico['registro'].isin(cargo_historico_proc_antetior['registro'])].drop_duplicates(subset='registro', keep='first')
    cargo_historico_proc_posterior['cod_cargo'] = cargo_historico_proc_posterior['anterior']
    cargo_historico_proc = pd.concat([cargo_historico_proc_antetior,cargo_historico_proc_posterior])
    cargo_historico_proc.reset_index(drop=True, inplace=True)
    cargo_historico_proc = cargo_historico_proc[['registro','cod_cargo']]
    movimentos_cargo_historico = pd.merge(movimentos_cargo_copy,cargo_historico_proc,on='registro', how='left').query('cargo_merge == "both"')
    movimentos_proc_cargo = pd.concat([movimentos_cargo_historico,movimentos_cargo])
    movimentos_proc_cargo.reset_index(drop=True, inplace=True)

    #---------------------------------UNUFICANDO O DATAFRAME--------------------------------------------#

    df_movimentacoes = movimentos.copy()
    df_movimentacoes = pd.merge(df_movimentacoes,trabalhador[['registro','cod_vinculo','p_nome','situacao','dtsituacao','dtdemissao','dtadmissao','dtnascimento','sexo','cod_instrucao','cod_estadocivil']], on='registro', how='inner')
    
    #------ORDENANDO DATAFRAMES PARA INTEGRAÇÃO CORRETA----------#
    df_movimentacoes.sort_values(by=['registro'],inplace=True)
    df_movimentacoes.reset_index(drop=True, inplace=True)

    movimentos_proc.sort_values(by=['registro'],inplace=True)
    movimentos_proc.reset_index(drop=True, inplace=True)

    movimentos_proc_sub.sort_values(by=['registro'],inplace=True)
    movimentos_proc_sub.reset_index(drop=True, inplace=True)

    movimentos_proc_localtrab.sort_values(by=['registro'],inplace=True)
    movimentos_proc_localtrab.reset_index(drop=True, inplace=True)

    movimentos_proc_cargo.sort_values(by=['registro'],inplace=True)
    movimentos_proc_cargo.reset_index(drop=True, inplace=True)

    #------CONVERTENDO TIPOS-------#
    df_movimentacoes['cod_esocial'] = pd.to_numeric(df_movimentacoes['cod_esocial'])
    tabela_esocial['codigo'] = pd.to_numeric(tabela_esocial['codigo'])
    contas['registro'] = pd.to_numeric(contas['registro'])
    
    df_movimentacoes['cod_divisao'] = movimentos_proc['cod_divisao']
    df_movimentacoes['cod_subdivisao'] = movimentos_proc_sub['cod_subdivisao']
    df_movimentacoes['cod_localtrab'] = movimentos_proc_localtrab['cod_localtrab']
    df_movimentacoes['cod_cargo'] = movimentos_proc_cargo['cod_cargo']

    df_movimentacoes = pd.merge(df_movimentacoes,tabela_divisao, left_on='cod_divisao', right_on='codigo', how='inner')
    df_movimentacoes = pd.merge(df_movimentacoes,tabela_subdivisao, left_on='cod_subdivisao', right_on='codigo', how='inner',suffixes=('_xsub','_ysub'))
    df_movimentacoes = pd.merge(df_movimentacoes,tabela_local_trabalho, left_on='cod_localtrab', right_on='codigo', how='inner')
    df_movimentacoes = pd.merge(df_movimentacoes,tabela_cargo, left_on='cod_cargo', right_on='codigo', how='inner',suffixes=('_xcar','_ycar'))
    df_movimentacoes = pd.merge(df_movimentacoes,tabela_vinculo, left_on='cod_vinculo', right_on='codigo', how='inner')
    df_movimentacoes = pd.merge(df_movimentacoes,tabela_esocial, left_on='cod_esocial', right_on='codigo', how='inner',suffixes=('_xeso','_yeso'))
    df_movimentacoes = pd.merge(df_movimentacoes,tabela_instrucao, left_on='cod_instrucao', right_on='codigo', how='inner')
    df_movimentacoes = pd.merge(df_movimentacoes,tabela_estadocivil, left_on='cod_estadocivil', right_on='codigo', how='inner')
    #CARREGANDO DADOS BANCARIOS
    df_movimentacoes = pd.merge(df_movimentacoes,contas, on='registro', how='inner')
    #REMOVENDO COLUNAS INDESEJADAS
    df_movimentacoes.drop(['codigo_x','codigo_y','codigo_xsub','codigo_ysub','codigo_xcar','codigo_ycar','codigo_xeso','codigo_yeso'],axis=1,inplace=True)

    return df_movimentacoes

def tabelas_complementares():
    
    sql_trabalhador = """SELECT T.REGISTRO,T.NOME,T.CPF,T.DTSITUACAO,T.DTADMISSAO,T.DTDEMISSAO,T.DTNASCIMENTO,T.SEXO,
    T.SITUACAO, S.NOME AS SITUACAO_NOME,
    T.DIVISAO AS COD_DIVISAO,D.NOME AS DIVISAO,
    T.SUBDIVISAO AS COD_SUBDIVISAO,SD.NOME AS SUBDIVISAO,
    T.LOCAL_TRABALHO AS COD_LOCAL_TRABALHO,
    T.VINCULO AS COD_VINCULO, V.NOME AS VINCULO,
    T.INSTRUCAO AS COD_INSTRUCAO,
    T.ESTADOCIVIL AS COD_ESTADOCIVIL, EC.NOME AS ESTADOCIVIL,
    T.CARGOINICIAL AS COD_CARGOINICIAL, CI.NOME AS CARGOINICIAL,
    T.CARGOATUAL AS COD_CARGOATUAL, CA.NOME AS CARGOATUAL
    FROM TRABALHADOR AS T
    INNER JOIN DIVISAO AS D ON T.DIVISAO = D.CODIGO
    INNER JOIN SUBDIVISAO AS SD ON T.SUBDIVISAO = SD.CODIGO
    INNER JOIN CARGOS AS CI ON T.CARGOINICIAL = CI.CODIGO
    INNER JOIN CARGOS AS CA ON T.CARGOATUAL = CA.CODIGO
    INNER JOIN ESTADOCIVIL AS EC ON T.ESTADOCIVIL = EC.CODIGO
    INNER JOIN VINCULO AS V ON T.VINCULO = V.CODIGO
    INNER JOIN SITUACOES AS S ON T.SITUACAO = S.CODIGO
    """
    sql_instrucao = """SELECT CODIGO,NOME FROM INSTRUCAO"""
    sql_local_trabalho = """SELECT CODIGO,NOME FROM LOCAL_TRABALHO"""
    sql_contas = """SELECT REGISTRO,BANCO,ITEM,TIPO,CONTA,DVCONTA,PADRAO,AGENCIA,DVAGENCIA FROM CONTASTRABALHADOR"""
    sql_referencia = """SELECT CODIGO,ANO,MES,TIPO,DTPAGTO,DTFECHA,PRIMEIRO_DIA,ULTIMO_DIA, ENCERRADO FROM REFERENCIA"""
    
    with engine.connect() as con:
        trabalhador = pd.read_sql_query(sql_trabalhador,con)
        contas = pd.read_sql_query(sql_contas,con)
        referencia = pd.read_sql_query(sql_referencia,con)
        instrucao = pd.read_sql_query(sql_instrucao,con)
        local_trabalho = pd.read_sql_query(sql_local_trabalho,con)
    
    return trabalhador,contas,referencia,instrucao,local_trabalho

app = Dash(__name__)



# assume you have a "long-form" data frame
# see https://plotly.com/python/px-arguments/ for more options
def generate_table(dataframe, max_rows=50):
    return html.Table([
        html.Thead(
            html.Tr([html.Th(col) for col in dataframe.columns])
        ),
        html.Tbody([
            html.Tr([
                html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
            ]) for i in range(min(len(dataframe), max_rows))
        ])
    ])

trabalhador,contas,referencia,instrucao,local_trabalho = tabelas_complementares()

app.layout = html.Div(children=[
    html.H1(children='Hello Dash'),

    html.Div(children='''
        Dash: A web application framework for your data.
    '''),
    generate_table(trabalhador),
])

if __name__ == '__main__':
    app.run_server(debug=True)
