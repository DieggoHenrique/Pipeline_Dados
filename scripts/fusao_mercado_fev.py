from processamento_dados import Dados


path_json = r'C:\Users\dieggo.araujo\Documents\Pythonista\Pipeline_Dados\data_raw\dados_empresaA.json'
path_csv = r'C:\Users\dieggo.araujo\Documents\Pythonista\Pipeline_Dados\data_raw\dados_empresaB.csv'

key_mapping = {'Nome do Item' : 'Nome do Produto',
                'Classificação do Produto' : 'Categoria do Produto',
                'Valor em Reais (R$)' : 'Preço do Produto (R$)',
                'Quantidade em Estoque' : 'Quantidade em Estoque',
                'Nome da Loja' : 'Filial',
                'Data da Venda' : 'Data da Venda'}

dados_empresaA = Dados(path=path_json, tipo_dados='json')
print(dados_empresaA.nome_colunas)
print(dados_empresaA.qtd_linhas)

dados_empresaB = Dados(path=path_csv, tipo_dados='csv')
print(dados_empresaB.nome_colunas)
print(dados_empresaB.qtd_linhas)

dados_empresaB.rename_columns(key_mapping=key_mapping)
print(dados_empresaB.nome_colunas)

dados_fusao = Dados.join(dados_empresaA, dados_empresaB)
print(dados_fusao.nome_colunas)
print(dados_fusao.qtd_linhas)

path_dados_combinados = 'C:/Users/dieggo.araujo/Documents/Pythonista/Pipeline_Dados/data_processed/dados_combinados_fev.csv'
dados_fusao.salvando_dados(path_dados_combinados)

help(Dados.salvando_dados)