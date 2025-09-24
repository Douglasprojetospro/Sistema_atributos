import pandas as pd
import re
from io import BytesIO

def processar_dados(data_df, config_df):
    """
    Processa os dados conforme as configurações
    """
    # Validar colunas necessárias
    required_data_cols = ['ID', 'Descrição']
    required_config_cols = ['Atributo', 'Variação', 'Padrão de reconhecimento']
    
    if not all(col in data_df.columns for col in required_data_cols):
        raise ValueError(f"Planilha de dados deve conter as colunas: {required_data_cols}")
        
    if not all(col in config_df.columns for col in required_config_cols):
        raise ValueError(f"Planilha de configurações deve conter as colunas: {required_config_cols}")
    
    # Criar cópia do DataFrame original
    result_df = data_df.copy()
    
    # Agrupar configurações por atributo
    config_groups = config_df.groupby('Atributo')
    
    for attr, group in config_groups:
        variations_list = []
        
        # Processar cada linha da planilha de dados
        for _, data_row in data_df.iterrows():
            descricao = str(data_row['Descrição']).lower()
            matched_variations = []
            
            # Verificar cada padrão do atributo atual
            for _, config_row in group.iterrows():
                patterns_str = str(config_row['Padrão de reconhecimento'])
                variation = str(config_row['Variação'])
                
                # Dividir os padrões e limpar
                patterns = [p.strip().lower() for p in patterns_str.split(',')]
                
                # Verificar se algum padrão está presente na descrição
                for pattern in patterns:
                    if pattern and pattern in descricao:
                        # Usar regex para busca exata de palavras
                        if re.search(r'\b' + re.escape(pattern) + r'\b', descricao):
                            if variation not in matched_variations:
                                matched_variations.append(variation)
                            break
            
            variations_list.append(', '.join(matched_variations) if matched_variations else '')
        
        result_df[attr] = variations_list
    
    return result_df

def to_excel(df):
    """
    Converte DataFrame para arquivo Excel
    """
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Relatório')
        
        # Formatação básica
        workbook = writer.book
        worksheet = writer.sheets['Relatório']
        
        # Ajustar largura das colunas
        for idx, col in enumerate(df.columns):
            max_len = max(df[col].astype(str).str.len().max(), len(col)) + 2
            worksheet.set_column(idx, idx, min(max_len, 50))
    
    return output.getvalue()

# Templates
def get_data_template():
    return pd.DataFrame({
        'ID': [1414, 2525],
        'Descrição': ['Ventilador de teto 110 amarelo biv', 'Luminária LED 220v branca']
    })

def get_config_template():
    return pd.DataFrame({
        'Atributo': ['Voltagem', 'Voltagem', 'Voltagem', 'Cor', 'Cor'],
        'Variação': ['110v', '220v', 'Bivolt', 'Amarelo', 'Branca'],
        'Padrão de reconhecimento': ['110,110v,127', '220,220v,227', 'bivolt,biv', 'amarelo,yellow', 'branca,white']
    })
