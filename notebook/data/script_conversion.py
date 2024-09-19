import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

# Caminho do arquivo CSV e do arquivo Parquet de destino
caminho_csv = '/mnt/c/Users/diego/Downloads/PaySim.csv'
arquivo_parquet_final = '/mnt/c/Users/diego/ml_projects/fraud_detection/notebook/data/PaySim.parquet'

# Definir o tamanho do chunk (por exemplo, 10.000 linhas)
chunk_size = 10000

# Inicializar variáveis
all_chunks = []
max_size = 99 * 1024 * 1024  # 99 MB em bytes
current_size = 0

# Função para calcular o tamanho de um arquivo Parquet
def get_parquet_size(table):
    buffer = pa.BufferOutputStream()
    pq.write_table(table, buffer, compression='snappy')
    return buffer.getvalue().size

# Ler o CSV em chunks
for chunk in pd.read_csv(caminho_csv, chunksize=chunk_size):
    # Converter o DataFrame para uma tabela PyArrow
    table = pa.Table.from_pandas(chunk)
    
    # Adicionar o chunk à lista
    all_chunks.append(table)
    
    # Verificar o tamanho acumulado dos chunks
    total_size = sum(get_parquet_size(tbl) for tbl in all_chunks)
    
    if total_size > max_size:
        # Se o tamanho acumulado exceder o máximo, escrever o arquivo Parquet
        # e limpar a lista de chunks
        final_table = pa.concat_tables(all_chunks)
        pq.write_table(final_table, arquivo_parquet_final, compression='snappy')
        
        # Limpar os chunks e reiniciar o tamanho
        all_chunks = []
        current_size = 0

# Escrever o restante dos chunks, se houver
if all_chunks:
    final_table = pa.concat_tables(all_chunks)
    pq.write_table(final_table, arquivo_parquet_final, compression='snappy')
