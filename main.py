import os

import pandas as pd
import numpy as np

directory_path = './input/'
output_path = "./output/"
extension = '.RET'
txt_files = [] # Lista de arquivos

tipo_registro_ignorado = ['5','9','0','1']

# Cria um DataFrame vazio para armazenar os campos de todos os arquivos
df = pd.DataFrame()


def insert_decimal_point(x):
    if isinstance(x, str) and '.' not in x:
        x = x[:-2] + '.' + x[-2:]
    return x

def get_files_with_ext(directory_path, extension):
    """
    Pega os arquivos dentro da pasta
    :param directory_path: define o path em que o arquivo está
    :param extension:  define a extensão do arquivo a ser captado
    :return: retorna o nome dos arquivos daquela extensão em uma lista
    """
    files_with_ext = []
    for file in os.listdir(directory_path):
        if file.endswith(extension):
            files_with_ext.append(file)
    return files_with_ext

txt_files = get_files_with_ext(directory_path, extension) # Instancia a função


def read_field(file, field_name, start_pos, end_pos):
    """
    Lê o campo especificado do arquivo e retorna um DataFrame.
    """
    with open(directory_path+file, 'r') as ret_file:
        # Lê todas as linhas
        lines = ret_file.readlines()
        new_lines = []
        for line in lines:
            if line[7:8] not in tipo_registro_ignorado:
                new_lines.append(line)
        lines = new_lines

        # Extrai o valor do campo para cada linha.
        field_values = [line[start_pos:end_pos].strip() for line in lines]
        # Cria um DataFrame com os valores lidos.
        df_field = pd.DataFrame(field_values, columns=[field_name])

        # Converte o tipo dos dados para o tipo apropriado.
        if 'vl' in field_name:
            modified_list = [] # Lista de suporte para a modificacao
            for i in field_values:
                i = i[:13]+ '.' + i[13:]
                modified_list.append(i)
            df_field = pd.DataFrame(modified_list, columns=[field_name])
            df_field = df_field.apply(pd.to_numeric, errors='coerce')

        # Converte o tipo dos dados para o tipo apropriado.
        if 'dt' in field_name:
            modified_list = [] # Lista de suporte para a modificacao
            for i in field_values:
                i = i[:2]+ '/' + i[2:4] + '/' + i[4:]
                modified_list.append(i)
            df_field = pd.DataFrame(modified_list, columns=[field_name])


    return df_field

for file in txt_files:

    df_cnpj = read_field(file,'cnpj', 203, 217)
    df_banco = read_field(file, 'banco', 20, 23)
    df_agencia = read_field(file, 'agencia', 24, 28)
    df_conta = read_field(file, 'conta', 29, 42)
    df_nome = read_field(file, 'nome', 43, 73)
    df_vl_prev_pgto= read_field(file, 'vl_prev_pgto', 119, 134)
    df_vl_real_operacao = read_field(file, 'vl_real_operacao', 119, 134)
    df_dt_prev_pgto = read_field(file, 'dt_prev_pgto', 92, 101)
    df_dt_real_operacao = read_field(file, 'dt_real_operacao', 154, 162)
    df_ocorrencia = read_field(file, 'ocorrencia', 230, 240)

    # CNPJ
    df = pd.merge(df, df_cnpj, left_index=True, right_index=True, how='outer')
    # Banco
    df = pd.merge(df, df_banco, left_index=True, right_index=True, how='outer')
    # Ag
    df = pd.merge(df, df_agencia, left_index=True, right_index=True,
                  how='outer')
    # Conta
    df = pd.merge(df, df_conta, left_index=True, right_index=True,
                  how='outer')
    # Nome
    df = pd.merge(df, df_nome, left_index=True, right_index=True,
                  how='outer')
    # Vl_prev_pgto
    df = pd.merge(df, df_vl_prev_pgto, left_index=True, right_index=True,
                  how='outer')
    # Vl_real_operacao
    df = pd.merge(df, df_vl_real_operacao, left_index=True, right_index=True,
                  how='outer')
    # Dt_prev_pgto
    df = pd.merge(df, df_dt_prev_pgto, left_index=True, right_index=True,
                  how='outer')
    # Dt_real_operacao
    df = pd.merge(df, df_dt_real_operacao, left_index=True, right_index=True,
                  how='outer')
    # Ocorrencia
    df = pd.merge(df, df_ocorrencia, left_index=True, right_index=True,
                  how='outer')

    print(df)


    # Escreve o arquivo de saída
    df.to_csv(output_path+'campos_agregados.csv',
              index=False,
              sep = ';',
              decimal=',')




