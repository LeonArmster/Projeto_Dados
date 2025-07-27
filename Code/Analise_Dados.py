# Biblitoecas
import pandas as pd
from sqlmodel import create_engine
import os
from dotenv import load_dotenv
from config import conexao, sql_dir

# Conectando ao banco
load_dotenv(dotenv_path=conexao)

## Configurações
usuario = os.getenv('usuario')
senha = os.getenv('senha')
host = os.getenv('host')
porta = os.getenv('porta')
database = os.getenv('bd')

database_url = f'postgresql://{usuario}:{senha}@{host}:{porta}/{database}'

engine = create_engine(database_url)

con = engine.connect()


# Carregando dados

## Função para ler os dados
def read_query(diretorio:str, nome_query:str, conexao:str) -> pd.DataFrame:
    """
    Obj: Função para carregar a query que do banco de dados em um dataframe

    diretorio: local onde estão os arquivos sql
    nome_query: nome do arquivo sql
    conexao: variável com a conexão ao banco

    Retorno: um dataframe do pandas

    """
    with open(diretorio/nome_query, 'r', encoding='utf-8') as select:
        query = select.read()

    df = pd.read_sql_query(query, con=conexao)

    return df

df_cliente = read_query(sql_dir, 'tb_clientes.sql', con)
df_categoria = read_query(sql_dir, 'tb_categoria.sql', con)
df_geolocalizacao = read_query(sql_dir, 'tb_geolocalizacao.sql', con)
df_itens = read_query(sql_dir, 'tb_itens_ordens.sql', con)
df_ordens = read_query(sql_dir, 'tb_ordens.sql', con)
df_pag = read_query(sql_dir, 'tb_pagamentos.sql', con)
df_produtos = read_query(sql_dir, 'tb_produtos.sql', con)
df_review = read_query(sql_dir, 'tb_review.sql', con)
df_vendedores = read_query(sql_dir, 'tb_vendedores.sql', con)


# Primeira questão: Qual o produto mais vendido?
df_produtos.head()

### Resposta: Ao avaliar o df podemos ver que não temos o nome dos produtos, somente a categoria deles. Então vamos direto para essa pergunta, qual a categoria de produtos mais vendido

# Segunda questão: Qual é a categoria de produto mais vendido?

## Juntando o df_itens com o df_produtos para identificar as categorias mais vendidas
### Filtrando somente as colunas que são necessárias
colunas = ['product_id', 'product_category_name']
df_vendas = pd.merge(df_itens, df_produtos[colunas], left_on='product_id', right_on='product_id', how='left')

## Vendo qual categoria vendeu mais
df_mais_vendidos = df_vendas.groupby('product_category_name',sort=False)[['order_id']].count()

## Criando coluna % vendas para saber o share de vendas de cada produto
df_mais_vendidos['% Vendas'] = (df_mais_vendidos['order_id'] / df_mais_vendidos['order_id'].sum() * 100).round(1).astype(str) + '%'
df_mais_vendidos

### Se aplicarmos a regra do 80 / 20 podemos ver que das 73 categorias de produtos que a base possuem, 15 categorias são responsáveis por 80% das vendas sendo elas:
### cama_mesa_banho	beleza_saude	esporte_lazer	moveis_decoracao	informatica_acessorios	utilidades_domesticas	relogios_presentes	telefonia	ferramentas_jardim	automotivo	brinquedos	cool_stuff	perfurmaria	bebes	eletronicos

# Terceira pergunta: Para qual local vende-se mais?