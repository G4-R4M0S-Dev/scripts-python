from validate_docbr import CPF
import csv, fdb
from datetime import datetime
con = fdb.connect(dsn='192.168.2.140:/Fiorilli/SIP_DADOS/SIP.FDB', user='fscsip', password='sip')
cur = con.cursor()



def deletar_dependentes():
    with open('Dependentes_validados_fopag.csv','r') as f:
        reader = csv.reader(f, delimiter=';', quoting=csv.QUOTE_NONE)
        for linha in reader:
            #---------------------------------------------------------------------#
            registro = linha[0]
            cur.execute(f'delete from dependentes where registro = {registro}')
    con.commit()

def recadastrar_tabela_dependentes():
    with open('Dependentes_validados_fopag.csv','r') as f, open('Dependentes_validados_fopag.csv','r') as p:
        reader = csv.reader(f, delimiter=';', quoting=csv.QUOTE_NONE)
        proximo = csv.reader(p, delimiter=';', quoting=csv.QUOTE_NONE)
        next(proximo)
        cpf = CPF()
        for linha, proxima_linha in zip(reader, proximo):
            #-----------------------CARREGANDO VARIAVEIS---------------------------------#
            registro = linha[0]
            nome = linha[1]
            cpf_dep = linha[2]
            dtnasc = datetime.strptime(linha[3],'%d/%m/%Y').date()
            sexo = linha[4]
            parentesco = linha[5]
            empresa = '001'
            #----------------------------------------------------------------------------#
            cur.execute(f"select count(*) from dependentes where cpf = '{cpf_dep}'")
            cpf_cadastrado_anteriormente = True if cur.fetchone()[0] != 0 else False
            cpf_valido = cpf.validate(cpf_dep)
            if not cpf_cadastrado_anteriormente and cpf_valido:
                cur.execute(f"select count(item) from dependentes where registro = '{registro}'")
                item = cur.fetchone()[0] + 1
                cur.execute(f'insert into dependentes (empresa,registro,item,nome,dtnasc,sexo,parentesco,cpf) values(?,?,?,?,?,?,?,?)',(empresa,registro,item,nome,dtnasc,sexo,parentesco,cpf_dep))
    con.commit()

i = 1
def recadastrar_tabela_trabalhador():
    with open('servidores_recadastro.csv', 'r') as f:
        reader = csv.reader(f, delimiter=';', quoting=csv.QUOTE_NONE)
        for linha in reader:
            #---------------------------------------------------------------------#
            cpf = linha[0].replace('.', '').replace('-', '')
            nome = linha[1]
            sexo = 'F' if linha[2] == 'Feminino' else 'M'
            nacionalidade = linha[3]
            data_nasc = datetime.strptime(linha[4],'%d/%m/%Y').date()
            estado_civil = linha[5]
            nome_pai = linha[6][:60]
            nome_mae = linha[7]
            endereco = linha[8][:70]
            numero = linha[9]
            bairro = linha[10]
            compl = linha[11][:20]
            cidade = linha[12][:30]
            uf = linha[13]
            email = linha[14]
            rn_cidade = linha[15][:30]
            instrucao = linha[16]
            raca = linha[17]
            #---------------------------------------------------------------------#
            # update = "UPDATE trabalhador SET nome = ?, sexo = ?, nacionalidade = ?, dtnascimento = ?, instrucao = ?, estadocivil = ?, nomepai = ?, nomemae = ?, endereco = ?, numero = ?, bairro = ?, compl = ?, cidade = ?, uf = ?, email = ?, rn_cidade = ?, raca = ? WHERE cpf = ? and situacao = 1"
            # cur.execute(update,(nome,sexo,nacionalidade,data_nasc,instrucao,estado_civil,nome_pai,nome_mae,endereco,numero,bairro,compl,cidade,uf,email,rn_cidade,raca,cpf))
            print(i)
            i += 1
            cur.execute(f"select nome,sexo,nacionalidade,dtnascimento,instrucao,estadocivil,nomepai,nomemae,endereco,numero,bairro,compl,cidade,uf,email,rn_cidade,raca from trabalhador where cpf = {cpf}")
            print(cur.fetchall())
            # break
    con.commit()

def update_dependentes_if_salfamilia():
    with open('DEPENDENTES_ORIGINAL.csv','r',encoding='utf-8') as f:
        reader = csv.reader(f,delimiter='#',quoting=csv.QUOTE_ALL)
        next(reader)
        for linha in reader:
            empresa = linha[0]
            registro = linha[1]
            item = linha[2]
            irrf = linha[8]
            salfamilia = linha[48]
            # print(empresa)
            # print(registro)
            # print(item)
            # print(irrf)
            # print(salfamilia)
            update = "UPDATE DEPENDENTES SET IRRF = ?, SALFAMILIA = ? WHERE EMPRESA = ? AND REGISTRO = ? AND ITEM = ?"
            cur.execute(update,(irrf,salfamilia,empresa,registro,item))
    con.commit()

if __name__ == "__main__":
    # deletar_dependentes()
    # recadastrar_tabela_dependentes()
    update_dependentes_if_salfamilia()
    con.close()