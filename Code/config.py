# Biblioteca
from pathlib import Path

# Caminho base do projeto
base_dir = Path(__file__).resolve().parent.parent.parent

# Diretório
arquivo_dir = base_dir/'Data'
conexao = 'conexao.env'