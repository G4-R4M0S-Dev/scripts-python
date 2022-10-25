import pandas as pd
pd.options.mode.chained_assignment = None
from sqlalchemy import create_engine
import sqlalchemy.sql.default_comparator   #hidden import para o pyinstaller funcionar
import fdb  #hidden import para o pyinstaller
import os
#TQDM

# engine = create_engine('firebird+fdb://fscsip:sip@localhost:3050/C:/Fiorilli/SIP_DADOS/SIP.FDB')
engine = create_engine('firebird+fdb://fscsip:sip@192.168.2.140:3050/C:/Fiorilli/SIP_DADOS/SIP.FDB')
# con = engine.connect()

def movimentos(ano,mes):    
    sql_movimentos = f"""
        SELECT M.REGISTRO, M.REFERENCIA, M.EVENTO,E.NOME,E.NATUREZA, M.VALOR, R.ANO,R.MES,R.TIPO,R.DTPAGTO,R.DTFECHA,R.ULTIMO_DIA,CLASSIFICACAO_ESOCIAL AS COD_ESOCIAL
        FROM MOVIMENTO AS M 
        INNER JOIN REFERENCIA AS R ON M.REFERENCIA = R.CODIGO
        INNER JOIN EVENTOS AS E ON E.CODIGO = M.EVENTO
        WHERE R.ANO = {ano} AND R.MES = {mes}
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
    
    if os.path.exists('trabalhador.csv'):
        os.remove('trabalhador.csv')
    if os.path.exists('contas.csv'):
        os.remove('contas.csv')
    if os.path.exists('referencia.csv'):
        os.remove('referencia.csv')
    if os.path.exists('instrucao.csv'):
        os.remove('instrucao.csv')
    if os.path.exists('local_trabalho.csv'):
        os.remove('local_trabalho.csv')
          
    trabalhador.to_csv('trabalhador.csv', index=False,encoding='utf-8',decimal=',')
    print("trabalhador.csv")
    contas.to_csv('contas.csv', index=False,encoding='utf-8',decimal=',')
    print("contas.csv")
    referencia.to_csv('referencia.csv', index=False,encoding='utf-8',decimal=',')
    print("referencia.csv")
    instrucao.to_csv('instrucao.csv', index=False,encoding='utf-8',decimal=',')
    print("instrucao.csv")
    local_trabalho.to_csv('local_trabalho.csv', index=False,encoding='utf-8',decimal=',')
    print("local_trabalho.csv")
        
#CALCULANDO TOTAIS
# total_vinculos = movimentos_proc.loc[movimentos_proc['natureza'] == 'V', 'valor'].sum()
#PREPARANDO PARA EXPORTAR
# cod_divisao = 0
# if cod_divisao != 0:
#     eventos = movimentos_proc[movimentos_proc['cod_divisao'] == cod_divisao]
#     total_proventos = movimentos_proc.loc[(movimentos_proc['natureza'] == 'P') & (movimentos_proc['cod_divisao'] == cod_divisao), 'valor'].sum()
#     quantidade_proventos = movimentos_proc.loc[(movimentos_proc['natureza'] == 'P') & (movimentos_proc['cod_divisao'] == cod_divisao), 'valor'].count()
#     total_deducoes = movimentos_proc.loc[(movimentos_proc['natureza'] == 'D') & (movimentos_proc['cod_divisao'] == cod_divisao), 'valor'].sum()
#     quantidade_deducoes = movimentos_proc.loc[(movimentos_proc['natureza'] == 'D') & (movimentos_proc['cod_divisao'] == cod_divisao), 'valor'].count()
#     total_faltas = movimentos_proc.loc[(movimentos_proc['classificacao_esocial'] == 9207) & (movimentos_proc['cod_divisao'] == cod_divisao), 'valor'].sum()
#     outros_valores = movimentos_proc.loc[(movimentos_proc['classificacao_esocial'] == 9989) & (movimentos_proc['cod_divisao'] == cod_divisao), 'valor'].sum()
    
# else:
#     eventos = movimentos_proc
#     total_proventos = movimentos_proc.loc[movimentos_proc['natureza'] == 'P', 'valor'].sum()
#     quantidade_proventos = movimentos_proc.loc[movimentos_proc['natureza'] == 'P', 'valor'].count()
#     total_deducoes = movimentos_proc.loc[movimentos_proc['natureza'] == 'D', 'valor'].sum()
#     quantidade_deducoes = movimentos_proc.loc[movimentos_proc['natureza'] == 'D', 'valor'].count()     
#     total_faltas = movimentos_proc.loc[movimentos_proc['classificacao_esocial'] == 9207, 'valor'].sum()
#     outros_valores = movimentos_proc.loc[movimentos_proc['classificacao_esocial'] == 9989, 'valor'].sum()

# total_descontos_liquido = total_deducoes - total_faltas - outros_valores
# total_bruto = total_proventos - total_faltas
# total_liquido = total_bruto - total_descontos_liquido

# eventos['qtd'] = eventos.groupby(['evento','nome'])['valor'].transform('count')
# eventos['total'] = eventos.groupby(['evento','nome'])['valor'].transform('sum')
# eventos = eventos[['evento','nome','natureza','cod_divisao','qtd','total','classificacao_esocial']].drop_duplicates(subset=['evento'])
# eventos = eventos.to_dict('records')

# movimentos_proc.to_csv('podeapagar.csv')

# return eventos, quantidade_proventos ,total_proventos, quantidade_deducoes ,total_deducoes
# movimentos_lotacao = pd.merge(movimentos,movimentos_copy, indicator='i', how='outer').query('i == "left_only"').drop('i', 1)

if __name__ == '__main__':
    print('#################################################')
    print('######CARREGAR INFORMAÇÕES FIORILLI PARA BI######')
    print('#################################################')
    dt_inicial = input('Digite a data inicial (mm/yyyy): ')
    dt_final = input('Digite a data final (mm/yyyy): ')
    
    try:
        dt_inicial_mes = int(dt_inicial[:2])
        dt_inicial_ano = int(dt_inicial[3:8])
        dt_final_mes = int(dt_final[:2])
        dt_final_ano = int(dt_final[3:8])
        mesi = dt_inicial_mes
        mesf = dt_final_mes
    except:
        print("Erro na data digitada, verifique se o formato foi correto (mes/ano)")
    
    if dt_final_mes < 1 or dt_final_mes > 12 or dt_inicial_mes < 1 or dt_inicial_mes > 12:
        try:
            x = 2/0
        except:
            print('Mês digitado não existe')

        
    print("CARREGANDO TABELAS COMPLEMENTARES")
    tabelas_complementares()
        
    print("CARREGANDO MOVIMENTAÇÕES")
    for ano in range(dt_inicial_ano,dt_final_ano+1):
        print('Carregando ano '+str(ano))
        if ano != dt_final_ano:
            dt_inicial_mes = 1
            dt_final_mes = 12
        else:
            dt_inicial_mes = mesi
            dt_final_mes = mesf
        for mes in range(dt_inicial_mes,dt_final_mes+1):
            print('Carregando mes '+str(mes))
            try:
                df = movimentos(ano,mes)
                if not os.path.exists('movimentos_fopag.csv'):
                    df.to_csv('movimentos_fopag.csv', index=False,encoding='utf-8',decimal=',')
                else:
                    local = pd.read_csv('movimentos_fopag.csv') #low_memory=False
                    if local.loc[(local['ano'] == ano) & (local['mes'] == mes)].empty:
                        df.to_csv('movimentos_fopag.csv', mode='a',header=False, index=False, encoding='utf-8',decimal=',')
                    else:
                        local.drop(local.loc[(local['ano'] == ano) & (local['mes'] == mes)].index,inplace=True)
                        local.to_csv('movimentos_fopag.csv', mode='w',header=True, index=False, encoding='utf-8',decimal=',')
                        df.to_csv('movimentos_fopag.csv', mode='a',header=False, index=False, encoding='utf-8',decimal=',')
            except IndexError:
                pass
    print("movimentos_fopag.csv")
    print("CARREGAMENTO FINALIZADO!!!!")