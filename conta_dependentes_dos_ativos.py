import csv


conta = 0
with open('TRABALHADOR_ATIVOS.csv','r', encoding='utf-8') as f:
        t_reader = csv.reader(f, delimiter='#',quotechar='"', quoting=csv.QUOTE_ALL)
        next(t_reader)
        for trabalhador in t_reader:
            # print(trabalhador[1])
            with open('DEPENDENTES_ATUALIZADO.csv','r', encoding='utf-8') as p:
                d_reader = csv.reader(p, delimiter='#',quotechar='"', quoting=csv.QUOTE_ALL)
                next(d_reader)
                for dependente in d_reader:
                    # print(dependente[1])
                    if trabalhador[1] == dependente[1]:
                        conta += 1

print(conta)
