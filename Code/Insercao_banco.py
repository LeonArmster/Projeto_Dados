# Bibliotecas
import pandas as pd
from sqlmodel import create_engine
from dotenv import load_dotenv
import os
from config import arquivo_dir, conexao

# Eliminar o limite de colunas
pd.set_option('display.max_columns', None)

# Criando conexão ao banco de dados
def conectar_servidor(usuario:str, senha:str, host:str, porta:str, database:str):

    user = usuario
    password = senha
    hostname = host
    port = porta
    bd = database

    database_url = f'postgresql://{user}:{password}@{hostname}:{port}/{bd}'
    engine = create_engine(database_url)
    conection = engine.connect()
    print('Conectado com sucesso')
    return conection


# Conectando ao banco de daods

## Puxando o caminho para conectar
load_dotenv(dotenv_path=conexao)

## Login
usuario = os.getenv('usuario')
senha = os.getenv('senha')
host = os.getenv('host')
porta = os.getenv('porta')
bd = os.getenv('bd')

## Conexão
conection = conectar_servidor(usuario, senha, host, porta, bd)

# Lendo os dados e inserindo no banco
#load_dotenv(dotenv_path=arquivo_dir)

## df ordens
df_order = pd.read_csv(r'C:\Users\leo-s\desktop\estudos\projeto_dados\Data\olist_orders_dataset.csv')
df_order.to_sql('Tb_Ordens', con=conection, index=False)

## df itens da ordem
df_order_itens = pd.read_csv(r'C:\Users\leo-s\desktop\estudos\projeto_dados\Data\olist_order_items_dataset.csv')
df_order_itens.to_sql('Tb__Itens_Ordens', con=conection, index=False)

## df geolocation
df_geolocation = pd.read_csv(r'C:\Users\leo-s\desktop\estudos\projeto_dados\Data\olist_geolocation_dataset.csv')
df_geolocation.to_sql('Tb_Geolocalizacao', con=conection, index=False)

## df pagamentos
df_payments = pd.read_csv(r'C:\Users\leo-s\desktop\estudos\projeto_dados\Data\olist_order_payments_dataset.csv')
df_payments.to_sql('Tb_Pagamentos', con=conection, index=False)

## df produtos
df_products = pd.read_csv(r'C:\Users\leo-s\desktop\estudos\projeto_dados\Data\olist_products_dataset.csv')
df_products.to_sql('Tb_Produtos', con=conection, index=False)

## df categoria produtos
df_category = pd.read_csv(r'C:\Users\leo-s\desktop\estudos\projeto_dados\Data\product_category_name_translation.csv')
df_category.to_sql('Tb_Categoria', con=conection, index=False)

## df vendedores
df_sellers = pd.read_csv(r'C:\Users\leo-s\desktop\estudos\projeto_dados\Data\olist_sellers_dataset.csv')
df_sellers.to_sql('Tb_Vendedores', con=conection, index=False)

## df revsao pedidos
df_review = pd.read_csv(r'C:\Users\leo-s\desktop\estudos\projeto_dados\Data\olist_order_reviews_dataset.csv')
df_review.to_sql('Tb_Review', con=conection, index=False)

## df clientes
df_cliente = pd.read_csv(r'C:\Users\leo-s\desktop\estudos\projeto_dados\Data\olist_customers_dataset.csv')
df_cliente.to_sql('Tb_Clientes', con=conection, index=False)