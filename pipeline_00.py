import os 
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

from duckdb import DuckDBPyRelation
from pandas import DataFrame

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
    print(dataframe_duckdb)
    print(type(dataframe_duckdb))
    return dataframe_duckdb

def transformacao(df: DuckDBPyRelation) -> DataFrame:
    df_transformado =duckdb.sql("SELECT *, quantidade * valor AS total_vendas FROM df").df()
    print(df_transformado)
    return df_transformado


if __name__ == "__main__":
    url_pasta ='https://drive.google.com/drive/folders/19flL9P8UV9aSu4iQtM6Ymv-77VtFcECP'
    diretorio_local = './pasta_gdown'
    # baixar_os_arquivos_do_google_drive(url_pasta, diretorio_local)
    arquivos = listar_arquivos_csv(diretorio_local)
    duckdb_dataframe = ler_csv(arquivos)
    transformacao(duckdb_dataframe)
