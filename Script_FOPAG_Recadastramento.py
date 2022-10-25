import csv, fdb
from datetime import datetime
con = fdb.connect(dsn='192.168.2.140:/Fiorilli/SIP_DADOS/SIP.FDB', user='fscsip', password='sip')
cur = con.cursor()
i = 1

def deletar_dependentes():
    with open('dependentes.csv','r') as f:
        reader = csv.reader(f, delimiter=';', quoting=csv.QUOTE_NONE)
        for linha in reader:
            #---------------------------------------------------------------------#
            cpf_pai = linha[0].replace('.', '').replace('-', '')
            cur.execute(f'select registro from trabalhador where cpf = {cpf_pai} and situacao = 1')
            registro = cur.fetchone()[0]
            
            cur.execute(f'delete from dependentes where registro = {registro}')
            break
    con.commit()

def recadastrar_tabela_dependentes():
    with open('dependentes.csv','r') as f:
        reader = csv.reader(f, delimiter=';', quoting=csv.QUOTE_NONE)
        for linha in reader:
            #---------------------------------------------------------------------#
            cpf_pai = linha[0].replace('.', '').replace('-', '')
            cur.execute(f'select registro from trabalhador where cpf = {cpf_pai} and situacao = 1')
            registro = cur.fetchone()[0]
            nome = linha[1]
            cpf_dep = linha[2].replace('.', '').replace('-', '')
            dtnasc = datetime.strptime(linha[3],'%d/%m/%Y').date()
            sexo = 'F' if linha[4] == 'Feminino' else 'M'
            parentesco = "{:02d}".format(int(linha[5]))
            for item in range(1,2):
                cur.execute(f'insert into dependentes (empresa,registro,item,nome,dtnasc,sexo,parentesco,cpf) values(?,?,?,?,?,?,?,?)',('001',registro,item,nome,dtnasc,sexo,parentesco,cpf_dep))
                con.commit()
                # print(parentesco)
            # cur.execute(f'select * from dependentes where registro = {registro}')
            # dependentes = cur.fetchall()
            # print(dependentes)
            break
        
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
    
def recadastrar_tabela_trabalhador_telefone():
    with open('telefone.csv', 'r') as f:
        reader = csv.reader(f, delimiter=';', quoting=csv.QUOTE_NONE)
        next(reader)
        for linha in reader:
            #---------------------------------------------------------------------#
            cpf = linha[0].replace('.', '').replace('-', '')
            telefone = linha[1]
            #---------------------------------------------------------------------#
            # update = "UPDATE trabalhador SET telefone = ? WHERE cpf = ? and situacao = 1"
            # cur.execute(update,(telefone,cpf))
            
            cur.execute(f"select nome, telefone from trabalhador where cpf = {cpf}")
            print(cur.fetchall())
            
            # break
    con.commit()

if __name__ == "__main__":
    # deletar_dependentes()
    # recadastrar_tabela_dependentes()
    recadastrar_tabela_trabalhador_telefone()
    con.close()