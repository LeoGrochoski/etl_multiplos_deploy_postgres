import os 
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

from duckdb import DuckDBPyRelation
from pandas import DataFrame

load_dotenv()

def baixar_os_arquivos_do_google_drive(url_pasta, diretorio_local):
    os.makedirs(diretorio_local, exist_ok=True)
    gdown.download_folder(url_pasta, output=diretorio_local, quiet=False, use_cookies=False)

def listar_arquivos_csv(diretorio):
    arquivos_csv = []
    todos_os_arquivos = os.listdir(diretorio)
    for arquivo in todos_os_arquivos:
        if arquivo.endswith(".csv"):
            caminho_completo = os.path.join(diretorio, arquivo)
            arquivos_csv.append(caminho_completo)
    return arquivos_csv

#Função que le arquivos e retorna um dataframe duckdb
def ler_csv(caminho_arquivo):
    dataframe_duckdb = duckdb.read_csv(caminho_arquivo)
    return dataframe_duckdb

def transformacao(df: DuckDBPyRelation) -> DataFrame:
    df_transformado =duckdb.sql("SELECT *, quantidade * valor AS total_vendas FROM df").df()
    return df_transformado

def salvar_banco_postgres(df_duckdb, tabela):
    DATABASE_URL = os.getenv("DATABASE_URL")
    engine = create_engine(DATABASE_URL)
    df_duckdb.to_sql(tabela, con=engine, if_exists='append', index=False)

if __name__ == "__main__":
    url_pasta ='https://drive.google.com/drive/folders/19flL9P8UV9aSu4iQtM6Ymv-77VtFcECP'
    diretorio_local = './pasta_gdown'
    # baixar_os_arquivos_do_google_drive(url_pasta, diretorio_local)
    lista_de_arquivos = listar_arquivos_csv(diretorio_local)

    for caminho_do_arquivo in lista_de_arquivos:
        duck_db_df = ler_csv(caminho_do_arquivo)
        pandas_df_transformado = transformacao(duck_db_df)
        salvar_banco_postgres(pandas_df_transformado, "vendas_calculado")

