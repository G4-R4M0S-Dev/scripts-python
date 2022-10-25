from fpdf import FPDF
import pandas as pd
pd.options.mode.chained_assignment = None
from sqlalchemy import create_engine
#TQDM

engine = create_engine('firebird+fdb://fscsip:sip@localhost:3050/C:/Fiorilli/SIP_DADOS/SIP.FDB')
con = engine.connect()

def carrega_divisao(codigo):
    consulta = con.execute(f"SELECT NOME FROM DIVISAO WHERE CODIGO = {codigo}")
    divisao_nome = consulta.fetchone()[0]
    return divisao_nome
    
def carrega_folha(codigo):
    consulta = con.execute(f"SELECT NOME FROM REFERENCIA_NOME WHERE TIPOREFERENCIA = {codigo}")
    folha_nome = consulta.fetchone()[0]
    return folha_nome

def cabecalho():
    print("###############################################################")
    print("###          PREFEITURA MUNICIPAL DE IMPERATRIZ - MA        ###")
    print("###  SUPERINTENDÊNCIA DE TECNOLOGIA DA INFORMAÇÃO - STI     ###")
    print("### SCRIPT PARA GERAR RELATÓRIO SINTÉTICO DE CUSTOS - FOPAG ###")
    print("###############################################################")
    print("DIGITE OS DADOS PARA EMISSÃO DO RELATÓRIO")

def tratamento(folha,divisao,mes,ano):
    sql = f"""
        SELECT M.REGISTRO, M.REFERENCIA, M.EVENTO,E.NOME,E.NATUREZA, M.VALOR, R.ANO,R.MES,R.TIPO,R.DTPAGTO,R.DTFECHA,R.ULTIMO_DIA,CLASSIFICACAO_ESOCIAL
        FROM MOVIMENTO AS M 
        INNER JOIN REFERENCIA AS R ON M.REFERENCIA = R.CODIGO
        INNER JOIN EVENTOS AS E ON E.CODIGO = M.EVENTO
        WHERE R.ANO = {ano} AND R.MES = {mes} AND R.TIPO = {folha}
        """
    with engine.connect() as con:
        movimentos = pd.read_sql_query(sql,con)

    sql = """
        SELECT REGISTRO, DIVISAO
        FROM TRABALHADOR
        """
    with engine.connect() as con:    
        lotacao = pd.read_sql_query(sql,con)

    sql = """
        SELECT REGISTRO, DIVISAO, ITEM, DHTRANSF, ANTERIOR
        FROM HISTTRAB_DIVISAO
        """
    with engine.connect() as con:
        lotacao_historico = pd.read_sql_query(sql,con)

    movimentos['dtpagto'] = pd.to_datetime(movimentos['dtpagto'])
    movimentos['ultimo_dia'] = pd.to_datetime(movimentos['ultimo_dia'])
    movimentos['dtfecha'] = pd.to_datetime(movimentos['dtfecha'])
    movimentos['registro'] = pd.to_numeric(movimentos['registro'])
    lotacao_historico['registro'] = pd.to_numeric(lotacao_historico['registro'])
    lotacao['registro'] = pd.to_numeric(lotacao['registro'])


    lotacao_historico_registros = lotacao_historico['registro'].drop_duplicates()
    movimentos_copy = pd.merge(movimentos,lotacao_historico_registros,on='registro', indicator='merge', how='left')
    #CARREGA DATAFRAME MOVIMENTOS COM OS DADOS DAS LOTACOES DE QUEM NUNCA MUDOU DE LOTACAO
    movimentos_lotacao = pd.merge(movimentos_copy,lotacao,on='registro', how='left').query('merge == "left_only"')

    data = movimentos['ultimo_dia'][0]
    #SELECIONA AS DIVISOES DE ACORDO COM A DARA
    selecao = lotacao_historico['dhtransf'] <= data
    #PEGA A ULTIMA MUDANCA DE LOTACAO ATE A DATA SELECIONADA
    lotacao_historico_proc_antetior = lotacao_historico[selecao].drop_duplicates(subset=['registro'], keep='last')
    #PEGA OS registroS POSTERIORES A DATA SELECIONADA
    lotacao_historico_proc_posterior = lotacao_historico[~lotacao_historico['registro'].isin(lotacao_historico_proc_antetior['registro'])].drop_duplicates(subset='registro', keep='first')
    #COLUNA ANTERIOR PASSA PARA COLUNA DIVISAO
    lotacao_historico_proc_posterior['divisao'] = lotacao_historico_proc_posterior['anterior']
    #JUNTANDO O PROCESSAMENTO EM UM UNICO DATAFRAME
    lotacao_historico_proc = pd.concat([lotacao_historico_proc_antetior,lotacao_historico_proc_posterior])
    #RESETANDO O INDEX DO NOVO DATAFRAME
    lotacao_historico_proc.reset_index(drop=True, inplace=True)
    #SELECIONANDO SOMENTE AS COLUNS registro E divisao PARA O DATAFRAME
    lotacao_historico_proc = lotacao_historico_proc[['registro','divisao']]


    #CARREGA DATAFRAME MOVIMENTOS COM OS DADOS DAS LOTACOES DE QUEM JÁ MUDOU DE LOTACAO
    movimentos_lotacao_historico = pd.merge(movimentos_copy,lotacao_historico_proc,on='registro', how='left').query('merge == "both"')

    #CRIANDO O DATAFRAME FINAL MOVIMENTOS_PROC COM DIVISOES ESTABELECIDAS
    movimentos_proc = pd.concat([movimentos_lotacao_historico,movimentos_lotacao])
    #RESETANDO O INDEX DO NOVO DATAFRAME
    movimentos_proc.reset_index(drop=True, inplace=True)
    movimentos_proc['divisao'] = pd.to_numeric(movimentos_proc['divisao'])


    #CALCULANDO TOTAIS
    # total_vinculos = movimentos_proc.loc[movimentos_proc['natureza'] == 'V', 'valor'].sum()

    #PREPARANDO PARA EXPORTAR
    if divisao != 0:
        eventos = movimentos_proc[movimentos_proc['divisao'] == divisao]
        total_proventos = movimentos_proc.loc[(movimentos_proc['natureza'] == 'P') & (movimentos_proc['divisao'] == divisao), 'valor'].sum()
        quantidade_proventos = movimentos_proc.loc[(movimentos_proc['natureza'] == 'P') & (movimentos_proc['divisao'] == divisao), 'valor'].count()
        total_deducoes = movimentos_proc.loc[(movimentos_proc['natureza'] == 'D') & (movimentos_proc['divisao'] == divisao), 'valor'].sum()
        quantidade_deducoes = movimentos_proc.loc[(movimentos_proc['natureza'] == 'D') & (movimentos_proc['divisao'] == divisao), 'valor'].count()
        total_faltas = movimentos_proc.loc[(movimentos_proc['classificacao_esocial'] == 9207) & (movimentos_proc['divisao'] == divisao), 'valor'].sum()
        outros_valores = movimentos_proc.loc[(movimentos_proc['classificacao_esocial'] == 9989) & (movimentos_proc['divisao'] == divisao), 'valor'].sum()
        
    else:
        eventos = movimentos_proc
        total_proventos = movimentos_proc.loc[movimentos_proc['natureza'] == 'P', 'valor'].sum()
        quantidade_proventos = movimentos_proc.loc[movimentos_proc['natureza'] == 'P', 'valor'].count()
        total_deducoes = movimentos_proc.loc[movimentos_proc['natureza'] == 'D', 'valor'].sum()
        quantidade_deducoes = movimentos_proc.loc[movimentos_proc['natureza'] == 'D', 'valor'].count()     
        total_faltas = movimentos_proc.loc[movimentos_proc['classificacao_esocial'] == 9207, 'valor'].sum()
        outros_valores = movimentos_proc.loc[movimentos_proc['classificacao_esocial'] == 9989, 'valor'].sum()
    
    total_descontos_liquido = total_deducoes - total_faltas - outros_valores
    total_bruto = total_proventos - total_faltas
    total_liquido = total_bruto - total_descontos_liquido
    
    eventos['qtd'] = eventos.groupby(['evento','nome'])['valor'].transform('count')
    eventos['total'] = eventos.groupby(['evento','nome'])['valor'].transform('sum')

    eventos = eventos[['evento','nome','natureza','divisao','qtd','total','classificacao_esocial']].drop_duplicates(subset=['evento'])
    eventos = eventos.to_dict('records')
    
    movimentos_proc.to_csv('podeapagar.csv')
    
    return eventos, quantidade_proventos ,total_proventos, quantidade_deducoes ,total_deducoes

    # movimentos_lotacao = pd.merge(movimentos,movimentos_copy, indicator='i', how='outer').query('i == "left_only"').drop('i', 1)

        
def gerar_tabela_pdf(folha,divisao,mes,ano):
    
    eventos,quantidade_proventos,total_proventos,quantidade_deducoes,total_deducoes = tratamento(folha,divisao,mes,ano)
    
    fetch = []
    proventos = []
    deducoes = []
    fields = ['evento','ano','mes','nome','quantidade','total']
    
    for evento in eventos:
        nome = evento['nome']
        quantidade = evento['qtd']
        total = evento['total']
        
        quantidade = "{:_.0f}".format(quantidade).replace('.',',').replace('_','.')
        total = "{:_.2f}".format(round(total,2)).replace('.',',').replace('_','.')
        
        fetch.append(evento['evento'])
        fetch.append(ano)
        fetch.append(mes)
        fetch.append(nome)
        fetch.append(quantidade)
        fetch.append(total)
        
        if evento['natureza'] == 'P':
            proventos.append(fetch)
        elif evento['natureza'] == 'D':
            deducoes.append(fetch)
            
        fetch = []
        
    quantidade_proventos = "{:_.0f}".format(quantidade_proventos).replace('.',',').replace('_','.')
    total_proventos = "{:_.2f}".format(round(total_proventos,2)).replace('.',',').replace('_','.')
    quantidade_deducoes = "{:_.0f}".format(quantidade_deducoes).replace('.',',').replace('_','.')
    total_deducoes = "{:_.2f}".format(round(total_deducoes,2)).replace('.',',').replace('_','.')
    
    return fields, proventos,deducoes, quantidade_proventos,total_proventos,quantidade_deducoes,total_deducoes

class PDF(FPDF):
    def header(self):
        #linhas nas bordas da página
        self.set_line_width(0.0)
        self.line(5.0,5.0,205.0,5.0) # top one
        # self.line(5.0,292.0,205.0,292.0) # bottom one
        self.line(5.0,283.0,205.0,283.0) # bottom two
        # self.line(90.0,283.0,120.0,283.0) # bottom two
        self.line(5.0,5.0,5.0,283.0) # left one
        self.line(205.0,5.0,205.0,283.0) # right one
        #################CABEÇALHO###################
        self.image('brasao_itz.png',8,6,16)
        # self.image('logo_itz.png',150,9,50)
        self.image('brasao_itz2.png',25,60,160)
        self.image('bandeira_itz.png',175,4,30)
        self.set_left_margin(0)
        self.set_font('helvetica','B', size=8)
        self.cell(0, 3, "PREFEITURA MUNICIPAL DE IMPERATRIZ - MA", align="C")
        self.ln()
        self.cell(0, 3, "SECRETARIA DE ADMINISTRAÇÃO E MODERNIZAÇÃO - SEAMO", align="C")
        self.ln()
        self.cell(0, 3, "SUPERINTENDÊNCIA DE TECNOLOGIA DA INFORMAÇÃO - STI", align="C")
        self.ln()
        self.cell(0, 3, "FOLHA DE PAGAMENTO - FOPAG", align="C")
        self.set_left_margin(10)
        self.line(5,27,205,27) # bottom cabeçalho
        #############################################
        # Performing a line break:
        self.ln(15)
    
    def footer(self):
        # Setting position at 1.5 cm from bottom:
        self.set_y(-15)
        # Setting font: helvetica italic 8
        self.set_font("Times", "I", 8)
        # Setting text color to gray:
        self.set_text_color(128)
        # Printing page number
        self.set_left_margin(5)
        self.cell(70, 10, f"FOLHA DE PAGAMENTO - FOPAG", align="L")
        self.cell(65, 10, f"IMPERATRIZ - MA", align="C")
        self.cell(65, 10, f"Página {self.page_no()}", align="R")
        self.set_left_margin(10)

        
    def improved_table(self, headings, rows, col_widths=(15, 11, 10, 98, 24, 32)):
        self.set_font('Courier','B', size=10)
        for col_width, heading in zip(col_widths, headings):
            self.cell(col_width, 7, heading, border=1, align="C")
        self.ln()
        self.set_font('Courier','', size=10)
        for row in rows:
            self.cell(col_widths[0], 4, row[0], border="LR", align="C")
            self.cell(col_widths[1], 4, row[1], border="LR", align="C")
            self.cell(col_widths[2], 4, row[2], border="LR", align="C")
            self.cell(col_widths[3], 4, row[3], border="LR", align="L")
            self.cell(col_widths[4], 4, row[4], border="LR", align="R")
            self.cell(col_widths[5], 4, row[5], border="LR", align="R")
            self.ln()
        # Closure line:
        self.set_font('Courier','B', size=10)
        
    def fechamento_tabela(self,quantidade_soma,total_soma,col_widths=(15, 11, 10, 98, 24, 32)):
        self.cell(sum(col_widths)-(col_widths[4]+col_widths[5]), 6, "Totais", border="LRBT",align="L")
        self.set_font('Courier','B', size=9)
        self.cell(col_widths[4], 6, quantidade_soma, border="LRBT",align="R")
        self.cell(col_widths[5], 6, total_soma, border="LRBT",align="R")

def gerar_pdf():
    cabecalho()
    print("OBS. DIGITE 00 PARA CONSIDERAR TODOS AS INFORMAÇÕES")
    
    folha = int(input("TIPO DA FOLHA: (1 - MENSAL / 4 - ADIANTAMENTO 13° / 7 - RECISÃO / 9 - FOLHA COMP. COM ENCARGOS)"))
    divisao = int(input("COD. DA DIVISÃO: "))
    mes = input("MES (mm): ")
    ano = input("ANO (yyyy): ")
    
    if divisao == 0:
        divisao_nome = 'TODAS AS SECRETARIAS'
    else:
        divisao_nome = carrega_divisao(divisao)
    
    if folha == 0:
        folha_nome = 'TODAS AS FOLHAS'
    else:
        folha_nome = carrega_folha(folha)
    
    
    mes_nome = ['JANEIRO','FEVEREIRO','MARÇO','ABRIL','MAIO','JUNHO','JULHO','AGOSTO','SETEMBRO','OUTUBRO','NOVEMBRO','DEZEMBRO']
    
    pdf = PDF(orientation='P', unit='mm', format='A4')
    pdf.add_page()
    
    ###       TITULO PRIMEIRA PAGINA      ###
    pdf.set_font('helvetica','B', size=10)
    if mes == '00':
        pdf.cell(0, 3, f"RELATÓRIO SINTÉTICO DE CUSTOS DO ANO DE {ano}", align="C")   #Título
    else:
        pdf.cell(0, 3, f"RELATÓRIO SINTÉTICO DE CUSTOS DO MÊS DE {mes_nome[int(mes)-1]} / {ano}", align="C")   #Título

    pdf.ln()
    pdf.ln()
    pdf.cell(0, 3, f"DIVISÃO {divisao} - {divisao_nome}", align="C")
    pdf.ln()
    pdf.cell(0, 5, f"{folha_nome}", align="C")
    pdf.line(45,43,165,43)
    pdf.ln(10)

    colunas, proventos, deducoes, quantidade_soma_proventos,total_soma_proventos,quantidade_soma_deducoes,total_soma_deducoes = gerar_tabela_pdf(folha,divisao,mes,ano)
    
    ###       CARREGA TABELA PROVENTOS     ###
    pdf.set_font('helvetica','B', size=10)
    pdf.cell(0, 3, f"1. - PROVENTOS", align="L")   #Título
    pdf.ln(5)
    pdf.improved_table(colunas, proventos)
    pdf.fechamento_tabela(quantidade_soma_proventos,total_soma_proventos)
    pdf.ln(10)
    
    ###       CARREGA TABELA DEDUÇÕES     ###
    pdf.set_font('helvetica','B', size=10)
    pdf.cell(0, 3, f"2. - DEDUÇÕES", align="L")   #Título
    pdf.ln(5)
    pdf.improved_table(colunas, deducoes)
    pdf.fechamento_tabela(quantidade_soma_deducoes,total_soma_deducoes)
    pdf.ln(10)
    
    # ###       CARREGA TABELA RESUMO    ###
    # pdf.set_font('helvetica','B', size=10)
    # pdf.cell(0, 3, f"3. - RESUMO", align="L")   #Título
    # pdf.ln(5)
    # pdf.improved_table(colunas, linhas,quantidade_soma,total_soma)
    # pdf.ln(10)
    
    pdf.output(F"f{folha}-d{divisao}-m{mes}-{ano}.pdf")

if __name__ == "__main__":
        gerar_pdf()
    