# Biblitoecas
import pandas as pd
from sqlmodel import create_engine
import os
from dotenv import load_dotenv
from config import conexao, sql_dir
import matplotlib.pyplot as plt
import seaborn as sns

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

## Função para criar gráficos de colunas
def bar_graph(coluna):
    plt.figure(figsize=(15,5))
    valores = coluna.value_counts().head(10)
    ax = sns.barplot(x=valores.values, y=valores.index)
    plt.show()



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

bar_graph(df_vendas['product_category_name'])

### Se aplicarmos a regra do 80 / 20 podemos ver que das 73 categorias de produtos que a base possuem, 15 categorias são responsáveis por 80% das vendas sendo elas:
### 1º cama_mesa_banho,	2º beleza_saude, 3º esporte_lazer,	4º moveis_decoracao, 5º informatica_acessorios, 6º utilidades_domesticas, 7º relogios_presentes, 8º telefonia, 9ºferramentas_jardim, 10º automotivo	brinquedos	cool_stuff	perfurmaria	bebes	eletronicos

# Terceira pergunta: Para qual local vende-se mais?
bar_graph(df_cliente['customer_state'])

bar_graph(df_cliente['customer_city'])

### Após analisar os gráficos podemos ver que os 3 estados que mais se vende são: SP, RJ, MG e que as cidades que mais se vendem são: São Paulo, Rio de Janeiro e Belo Horizonte

# Quarta pergunta: Qual a principal forma de pagamento?
df_share_pagamento = df_pag.groupby('payment_type')[['order_id']].count()
df_share_pagamento['Share'] = (df_share_pagamento['order_id'] / df_share_pagamento['order_id'].sum() * 100).round(1).astype(str) + '%'

### Ao analisar o df share_pagamento podemos ver que 74% dos pagamentos foram feitos no cartão de crédito e 19% foi feito em boleto sendo os 7% restantes em outras formas de pagamento

# há algum cliente que compra mais?
bar_graph(df_cliente['customer_unique_id'])

### Após analisar o gráfico podemos ver que não temos um cliente em destaque

# Há algum cliente que tenha um valor de compra mais elevado em destaque?
df_compras_gerais = pd.merge(df_ordens, df_itens, how='left')
df_compras_gerais['total_price'] = df_compras_gerais['price'] + df_compras_gerais['freight_value']
df_compras_gerais = pd.merge(df_compras_gerais, df_cliente, how='left')

df_valor_clientes = df_compras_gerais.groupby('customer_unique_id')[['total_price']].sum()

df_valor_clientes.describe()

### Após analisar os valores de compra, vemos que não temos muitos valores de destaque, sendo a média 164,87 gasto tendo um outlier de 13664, porém mesmo no terceiro quartil, os valores ficam em 182,23 e no 1º quartil um valor de 62,39 indicando que os valores não estão desviando muito

# Se possui datas de vendas houve aumento ou queda em algum produto ou categoria especifico em algum período?
## Criando a coluna data
df_ordens['purchase_date'] = pd.to_datetime(df_ordens['order_purchase_timestamp']).dt.date

## Agrupando a quantidade de compras por dia
df_qtd_compras_dia = df_ordens.groupby('purchase_date')[['order_id']].count()

## Ordenando do maior para o menor para saber os dias que mais tiveram compras
df_qtd_compras_dia.sort_values('order_id', ascending=False)

### Podemos ver que as datas que mais venderam foram especificamente 24,25,26,27,28 e 29 de novembro de 2017, essa data foi blackfriday

# Algum dia da semana vende mais?
## Criando a coluna dia da semana
df_ordens['day_name'] = pd.to_datetime(df_ordens['order_purchase_timestamp']).dt.day_name()

df_ordens.groupby('day_name')[['order_id']].count().sort_values('order_id', ascending=False)

### Ao analisar o dataframe, vemos que os dias que menos compram são nos finais de semana e o dia que mais compram é na segunda-feira