import json
import csv

from processamento_dados import Dados


# def leitura_json(path_json):
#     dados_json = []
#     with open(path_json, 'r') as file:
#         dados_json = json.load(file)
#     return dados_json


# def leitura_csv(path_csv):
#     dados_csv = []
#     with open(path_csv, 'r' ) as file:
#         spamreader = csv.DictReader(file, delimiter=',')
#         for row in spamreader:
#             dados_csv.append(row)
            
#     return dados_csv


# def leitura_dados(path, tipo_arquivo):
    
#     dados = []
#     if tipo_arquivo == 'csv':
#         dados = leitura_csv(path)
#     elif tipo_arquivo == 'json':
#         dados = leitura_json(path)
        
#     return dados


# def get_columns(dados):
#     return list(dados[0].keys())


# def rename_columns(dados, key_map):
#     new_dados_csv = []
    
#     for old_dict in dados:
#         dict_temp = {}
#         for old_key, value in old_dict.items():
#             dict_temp[key_mapping[old_key]] = value
#         new_dados_csv.append(dict_temp)
    
#     return new_dados_csv


def size_data(dados):
    return len(dados)


def join(dados_a, dados_b):
    combined_list = []
    combined_list.extend(dados_a)
    combined_list.extend(dados_b)
    
    return combined_list


def transformando_dados_tabela(dados, nomes_colunas):
    dados_combinados_tabela = [nomes_colunas]
    
    for row in dados:
        linha = []
        for colunas in nomes_colunas:
            linha.append(row.get(colunas, 'Indisponível'))
        dados_combinados_tabela.append(linha)
        
    return dados_combinados_tabela


def salvando_dados(dados, path):
    with open(path, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(dados)



path_json = '/Users/dieggo.araujo/Desktop/Documentos/pipeline_dados/data_raw/dados_empresaA.json'
path_csv = '/Users/dieggo.araujo/Desktop/Documentos/pipeline_dados/data_raw/dados_empresaB.csv'

key_mapping = {'Nome do Item': 'Nome do Produto',
                'Classificação do Produto': 'Categoria do Produto',
                'Valor em Reais (R$)': 'Preço do Produto (R$)',
                'Quantidade em Estoque': 'Quantidade em Estoque',
                'Nome da Loja': 'Filial',
                'Data da Venda': 'Data da Venda'}

dados_empresaA = Dados(path=path_json, tipo_dados='json')
print(dados_empresaA.nome_colunas)

dados_empresaB = Dados(path=path_csv, tipo_dados='csv')
print(dados_empresaB.nome_colunas)


dados_empresaB.rename_columns(key_mapping=key_mapping)
print(dados_empresaB.nome_colunas)

# # Iniciando a Leitura
# dados_json = leitura_json(path_json)
# nome_colunas_json = get_columns(dados_json)
# print(nome_colunas_json)
# tamanho_dados_json = size_data(dados_json)
# print(tamanho_dados_json)

# dados_csv = leitura_csv(path_csv)
# nome_colunas_csv = get_columns(dados=dados_csv)
# print(nome_colunas_csv)
# tamanho_dados_csv = size_data(dados_csv)
# print(tamanho_dados_csv)

# # Transformação dos dados



# dados_csv = rename_columns(dados_csv, key_mapping)
# nome_colunas_csv = get_columns(dados_csv)
# print(nome_colunas_csv)

# dados_fusao = join(dados_csv, dados_json)
# nome_colunas_fusao = get_columns(dados_fusao)
# tamanho_dados_fusao = size_data(dados_fusao)
# print(f'Dados fusáo: {dados_fusao}')
# print(f'Nome Colunas dados fusáo: {nome_colunas_fusao}')
# print(f'Numero de Registros: {tamanho_dados_fusao}')



# # Salvando os dados
# dados_fusao_tabela = transformando_dados_tabela(dados_fusao, nome_colunas_fusao)

# path_dados_combinados = '/Users/dieggo.araujo/Desktop/Documentos/pipeline_dados/data_processed/dados_combinados.csv'
# salvando_dados(dados_fusao_tabela, path_dados_combinados)

# print(path_dados_combinados)