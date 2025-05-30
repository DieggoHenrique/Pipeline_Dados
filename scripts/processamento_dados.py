import json
import csv


class Dados:
    
    """Classe para manipulação de dados em diferentes formatos (JSON, CSV, lista).
    
    Attributes:
        path (str): Caminho do arquivo ou lista de dados.
        tipo_dados (str): Tipo dos dados ('csv', 'json' ou 'list').
        dados (list): Dados carregados em memória.
        nome_colunas (list): Nomes das colunas/atributos.
        qtd_linhas (int): Quantidade de registros nos dados.
    """
    
    def __init__(self, path, tipo_dados):
        """Inicializa a classe Dados e carrega os dados.
        
        Args:
            path (str/list): Caminho do arquivo ou lista de dicionários.
            tipo_dados (str): Tipo dos dados ('csv', 'json' ou 'list').
            
        Example:
            >>> dados = Dados('dados.csv', 'csv')
            >>> dados_json = Dados('dados.json', 'json')
            >>> dados_lista = Dados([{'nome': 'João'}], 'list')
        """
        self.path = path
        self.tipo_dados = tipo_dados 
        self.dados = self.leitura_dados()
        self.nome_colunas = self.get_columns()
        self.qtd_linhas = self.size_data()
        


    def leitura_json(self):
        """Lê um arquivo JSON e retorna os dados como lista de dicionários.
        
        Returns:
            list: Lista de dicionários com os dados do JSON.
            
        Example:
            >>> dados = Dados('dados.json', 'json')
            >>> print(dados.dados[:2])  # Primeiros 2 registros
        """
        dados_json = []
        with open(self.path, 'r', encoding='utf-8') as file:
            dados_json = json.load(file)
        return dados_json


    def leitura_csv(self):
        """Lê um arquivo CSV e retorna os dados como lista de dicionários.
        
        Returns:
            list: Lista de dicionários com os dados do CSV.
            
        Example:
            >>> dados = Dados('dados.csv', 'csv')
            >>> print(dados.nome_colunas)  # Nome das colunas
        """
        dados_csv = []
        with open(self.path, 'r', encoding='utf-8') as file:
            spamreader = csv.DictReader(file, delimiter=',')
            for row in spamreader:
                dados_csv.append(row)
                
        return dados_csv


    def leitura_dados(self):
        """Método interno para carregar dados conforme o tipo especificado.
        
        Returns:
            list: Dados carregados no formato apropriado.
            
        Example:
            >>> dados = Dados('dados.csv', 'csv')
            >>> print(len(dados.dados))  # Quantidade de registros
        Observação: os formatos aceitos são: 'json' e 'csv
        """
        dados = []
        if self.tipo_dados == 'csv':
            dados = self.leitura_csv()
            
        elif self.tipo_dados == 'json':
            dados = self.leitura_json()
            
        elif self.tipo_dados == 'list':
            dados = self.path
            self.path = 'lista_memoria'
            
        return dados


    def get_columns(self):
        """Obtém os nomes das colunas/atributos dos dados.
        
        Returns:
            list: Lista com os nomes das colunas.
            
        Example:
            >>> dados = Dados('dados.csv', 'csv')
            >>> print(dados.get_columns())
            ['nome', 'idade', 'cidade']
        """
        return list(self.dados[-1].keys())


    def rename_columns(self, key_mapping):
        """Renomeia as colunas dos dados conforme o mapeamento fornecido.
        
        Args:
            key_mapping (dict): Dicionário com mapeamento {nome_antigo: nome_novo}.
            
        Example:
            >>> dados = Dados('dados.csv', 'csv')
            >>> mapeamento = {'nome': 'Nome Completo', 'idade': 'Idade'}
            >>> dados.rename_columns(mapeamento)
            >>> print(dados.nome_colunas)
            ['Nome Completo', 'Idade']
        """
        new_dados = []
        
        for old_dict in self.dados:
            dict_temp = {}
            for old_key, value in old_dict.items():
                dict_temp[key_mapping[old_key]] = value
            new_dados.append(dict_temp)
        
        self.dados = new_dados
        self.nome_colunas = self.get_columns()


    def size_data(self):
        """Retorna a quantidade de registros nos dados.
        
        Returns:
            int: Número de registros/linhas.
            
        Example:
            >>> dados = Dados('dados.csv', 'csv')
            >>> print(dados.size_data())
            100
        """
        return len(self.dados)


    def join(dados_a, dados_b):
        """Combina dados de duas fontes em um novo objeto Dados.
        
        Args:
            dados_a (Dados): Primeiro conjunto de dados.
            dados_b (Dados): Segundo conjunto de dados.
            
        Returns:
            Dados: Novo objeto com dados combinados.
            
        Example:
            >>> dados1 = Dados('dados1.csv', 'csv')
            >>> dados2 = Dados('dados2.json', 'json')
            >>> dados_combinados = Dados.join(dados1, dados2)
        """
        combined_list = []
        combined_list.extend(dados_a.dados)
        combined_list.extend(dados_b.dados)
        
        return Dados(combined_list, 'list')


    def transformando_dados_tabela(self):
        """Converte os dados para formato tabular (lista de listas).
        
        Returns:
            list: Lista de listas onde a primeira linha contém os cabeçalhos.
            
        Example:
            >>> dados = Dados('dados.csv', 'csv')
            >>> tabela = dados.transformando_dados_tabela()
            >>> print(tabela[:2])  # Cabeçalho + primeira linha
        """
        dados_combinados_tabela = [self.nome_colunas]
        
        for row in self.dados:
            linha = []
            for colunas in self.nome_colunas:
                linha.append(row.get(colunas, 'Indisponível'))
            dados_combinados_tabela.append(linha)
            
        return dados_combinados_tabela


    def salvando_dados(self, path):
        """Salva os dados em um arquivo CSV.
        
        Args:
            path (str): Caminho onde o arquivo será salvo.
            
        Example:
            >>> dados = Dados('dados.json', 'json')
            >>> dados.salvando_dados('dados_convertidos.csv')
        """
        
        dados_combinado_tabela = self.transformando_dados_tabela()
        
        with open(path, 'w', encoding='utf-8', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(dados_combinado_tabela)