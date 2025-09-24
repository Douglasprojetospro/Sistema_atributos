import streamlit as st
import pandas as pd
from io import BytesIO
import re
import tempfile
import os
from pathlib import Path
import time

# ==================================================
# CONFIGURAÇÕES INICIAIS E OTIMIZAÇÕES
# ==================================================

# Configurações específicas para Render
def is_render():
    """Detecta se está executando no ambiente Render"""
    return 'RENDER' in os.environ or ('HOSTNAME' in os.environ and 'render' in os.environ['HOSTNAME'])

# Configurações da página
st.set_page_config(
    page_title="Processador de Planilhas - Otimizado", 
    page_icon="📊", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configurações do pandas para melhor performance
pd.set_option('mode.chained_assignment', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)

# Otimizações específicas para Render
if is_render():
    MAX_ROWS_RENDER = 50000  # Limite conservador para Render
    st.markdown("""
    <style>
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    .stProgress > div > div > div > div {
        background-color: #1f77b4;
    }
    </style>
    """, unsafe_allow_html=True)
else:
    MAX_ROWS_RENDER = 200000  # Limite maior para execução local

# ==================================================
# FUNÇÕES AUXILIARES
# ==================================================

def to_excel(df):
    """
    Converte DataFrame para Excel em memória
    """
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Resultado')
        
        # Formatação básica
        workbook = writer.book
        worksheet = writer.sheets['Resultado']
        format_header = workbook.add_format({'bold': True, 'bg_color': '#366092', 'font_color': 'white'})
        
        # Formatar cabeçalho
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, format_header)
            
    output.seek(0)
    return output.getvalue()

def ler_arquivo_eficiente(arquivo):
    """
    Lê arquivo Excel de forma otimizada para diferentes tamanhos
    """
    try:
        # Criar arquivo temporário
        temp_dir = tempfile.mkdtemp()
        temp_path = os.path.join(temp_dir, arquivo.name)
        
        with open(temp_path, 'wb') as f:
            f.write(arquivo.getvalue())
        
        # Verificar tamanho do arquivo
        tamanho_mb = os.path.getsize(temp_path) / (1024 * 1024)
        st.info(f"📁 Tamanho do arquivo: {tamanho_mb:.2f} MB")
        
        # Estratégias diferentes baseadas no tamanho
        if tamanho_mb > 50:
            st.warning("⚡ Arquivo grande detectado. Usando modo de leitura otimizado...")
            
            # Ler metadados primeiro
            xl = pd.ExcelFile(temp_path)
            primeira_linha = pd.read_excel(temp_path, nrows=1)
            
            # Verificar colunas necessárias
            colunas_necessarias = ['ID', 'Descrição']
            colunas_disponiveis = [col for col in colunas_necessarias if col in primeira_linha.columns]
            
            if len(colunas_disponiveis) == len(colunas_necessarias):
                # Ler apenas colunas necessárias
                df = pd.read_excel(
                    temp_path,
                    usecols=colunas_necessarias,
                    dtype={'ID': 'str', 'Descrição': 'str'},
                    engine='openpyxl'
                )
            else:
                # Ler todas as colunas
                df = pd.read_excel(temp_path, engine='openpyxl')
                
        else:
            # Leitura normal para arquivos menores
            df = pd.read_excel(temp_path, engine='openpyxl')
        
        # Limpeza
        os.remove(temp_path)
        os.rmdir(temp_dir)
        
        return df
        
    except Exception as e:
        st.error(f"❌ Erro ao ler arquivo: {str(e)}")
        # Tentativa de fallback
        try:
            return pd.read_excel(arquivo, engine='openpyxl')
        except:
            return None

def processar_em_lotes(data_df, config_df, tamanho_lote=1000):
    """
    Processa os dados em lotes para economizar memória
    """
    # Pré-processar configurações
    config_groups = config_df.groupby('Atributo')
    config_dict = {}
    
    for attr, group in config_groups:
        config_dict[attr] = []
        for _, row in group.iterrows():
            patterns = [p.strip().lower() for p in str(row['Padrão de reconhecimento']).split(',') if p.strip()]
            config_dict[attr].append({
                'variation': str(row['Variação']),
                'patterns': patterns
            })
    
    # Processar em lotes
    resultados = []
    total_linhas = len(data_df)
    
    for i in range(0, total_linhas, tamanho_lote):
        lote = data_df.iloc[i:i + tamanho_lote].copy()
        
        for attr, configs in config_dict.items():
            coluna_resultado = []
            
            for _, linha in lote.iterrows():
                descricao = str(linha['Descrição']).lower()
                variacoes_encontradas = []
                
                for config in configs:
                    for pattern in config['patterns']:
                        if pattern and re.search(r'\b' + re.escape(pattern) + r'\b', descricao):
                            if config['variation'] not in variacoes_encontradas:
                                variacoes_encontradas.append(config['variation'])
                            break
                
                coluna_resultado.append(', '.join(variacoes_encontradas) if variacoes_encontradas else '')
            
            lote[attr] = coluna_resultado
        
        resultados.append(lote)
        
        # Atualizar progresso
        progresso = min((i + len(lote)) / total_linhas, 1.0)
        yield progresso, lote
    
    # Retornar resultado final
    if resultados:
        result_df = pd.concat(resultados, ignore_index=True)
        yield 1.0, result_df
    else:
        yield 1.0, pd.DataFrame()

def processamento_direto(data_df, config_df):
    """
    Processamento direto para arquivos pequenos
    """
    config_groups = config_df.groupby('Atributo')
    result_df = data_df.copy()
    
    total_attrs = len(config_groups)
    progress_bar = st.progress(0)
    
    for i, (attr, group) in enumerate(config_groups):
        progress_bar.progress(i / total_attrs)
        
        variations_list = []
        for _, data_row in data_df.iterrows():
            descricao = str(data_row['Descrição']).lower()
            matched_variations = []
            
            for _, config_row in group.iterrows():
                patterns = [p.strip().lower() for p in str(config_row['Padrão de reconhecimento']).split(',') if p.strip()]
                variation = str(config_row['Variação'])
                
                for pattern in patterns:
                    if pattern and re.search(r'\b' + re.escape(pattern) + r'\b', descricao):
                        if variation not in matched_variations:
                            matched_variations.append(variation)
                        break
            
            variations_list.append(', '.join(matched_variations) if matched_variations else '')
        
        result_df[attr] = variations_list
    
    progress_bar.progress(1.0)
    return result_df

# ==================================================
# INTERFACE DO USUÁRIO
# ==================================================

st.title("📊 Processador de Planilhas - Otimizado para Grandes Arquivos")
st.markdown("""
*Processe grandes volumes de dados de forma eficiente com reconhecimento de padrões em descrições de produtos.*
""")
st.markdown("---")

# ==================================================
# SEÇÃO DE TEMPLATES
# ==================================================

st.subheader("📋 Modelos para Download")

col1, col2 = st.columns(2)

with col1:
    # Template de dados
    data_template = pd.DataFrame({
        'ID': [1414, 2525, 3636, 4747],
        'Descrição': [
            'Ventilador de teto 110 amarelo biv', 
            'Luminária LED 220v branca',
            'Lâmpada LED 12W 127V quente',
            'Sensor movimento 220v preto'
        ]
    })
    
    st.download_button(
        "📥 Baixar modelo de dados", 
        to_excel(data_template), 
        file_name="modelo_dados.xlsx",
        help="Modelo da planilha com colunas ID e Descrição",
        type="secondary"
    )
    
    st.dataframe(data_template, use_container_width=True)

with col2:
    # Template de configurações
    config_template = pd.DataFrame({
        'Atributo': ['Voltagem', 'Voltagem', 'Voltagem', 'Cor', 'Cor', 'Tipo', 'Tipo'],
        'Variação': ['110v', '220v', 'Bivolt', 'Amarelo', 'Branca', 'LED', 'Sensor'],
        'Padrão de reconhecimento': [
            '110,110v,127', 
            '220,220v,227', 
            'bivolt,biv', 
            'amarelo,yellow', 
            'branca,white',
            'led,lâmpada,light',
            'sensor,detector,movimento'
        ]
    })
    
    st.download_button(
        "📥 Baixar modelo de configurações", 
        to_excel(config_template), 
        file_name="modelo_config.xlsx",
        help="Modelo da planilha com colunas Atributo, Variação e Padrão de reconhecimento",
        type="secondary"
    )
    
    st.dataframe(config_template, use_container_width=True)

st.markdown("---")

# ==================================================
# SEÇÃO DE UPLOAD
# ==================================================

st.subheader("📤 Upload dos Arquivos")

upload_col1, upload_col2 = st.columns(2)

with upload_col1:
    st.info("""
    **📊 Planilha de Dados:**
    - Colunas obrigatórias: **ID** e **Descrição**
    - Suporta outras colunas adicionais
    - Formatos suportados: XLSX
    """)
    data_file = st.file_uploader("Planilha de dados", type="xlsx", key="data_upload")

with upload_col2:
    st.info("""
    **⚙️ Planilha de Configurações:**
    - Colunas obrigatórias: **Atributo**, **Variação**, **Padrão de reconhecimento**
    - Padrões separados por vírgula
    - Formatos suportados: XLSX
    """)
    config_file = st.file_uploader("Planilha de configurações", type="xlsx", key="config_upload")

# ==================================================
# PROCESSAMENTO PRINCIPAL
# ==================================================

if data_file and config_file:
    try:
        # Ler arquivos
        st.subheader("📖 Lendo Arquivos...")
        
        with st.spinner("Carregando arquivos..."):
            config_df = pd.read_excel(config_file)
            data_df = ler_arquivo_eficiente(data_file)
        
        if data_df is None:
            st.error("❌ Erro ao ler arquivo de dados. Verifique o formato do arquivo.")
            st.stop()
        
        # Validar colunas
        required_data_cols = ['ID', 'Descrição']
        required_config_cols = ['Atributo', 'Variação', 'Padrão de reconhecimento']
        
        if not all(col in data_df.columns for col in required_data_cols):
            st.error(f"❌ Planilha de dados deve conter as colunas: {required_data_cols}")
            st.write("📋 Colunas encontradas:", list(data_df.columns))
            st.stop()
            
        if not all(col in config_df.columns for col in required_config_cols):
            st.error(f"❌ Planilha de configurações deve conter as colunas: {required_config_cols}")
            st.write("📋 Colunas encontradas:", list(config_df.columns))
            st.stop()
        
        # Mostrar estatísticas
        st.subheader("📊 Estatísticas dos Dados")
        
        info_col1, info_col2, info_col3, info_col4 = st.columns(4)
        
        with info_col1:
            st.metric("Linhas de dados", f"{len(data_df):,}")
        
        with info_col2:
            st.metric("Colunas de dados", len(data_df.columns))
        
        with info_col3:
            atributos_unicos = len(config_df['Atributo'].unique())
            st.metric("Atributos configurados", atributos_unicos)
        
        with info_col4:
            tamanho_memoria = data_df.memory_usage(deep=True).sum() / (1024 * 1024)
            st.metric("Uso de memória (MB)", f"{tamanho_memoria:.1f}")
        
        # Configurações de processamento
        st.subheader("⚙️ Configurações de Processamento")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if is_render():
                if len(data_df) > MAX_ROWS_RENDER:
                    st.error(f"⚠️ Arquivo muito grande para o Render (limite: {MAX_ROWS_RENDER} linhas)")
                    usar_lotes = True
                else:
                    usar_lotes = len(data_df) > 5000
            else:
                usar_lotes = st.checkbox("Usar processamento em lotes", 
                                       value=len(data_df) > 5000,
                                       help="Recomendado para arquivos grandes")
        
        with col2:
            if usar_lotes:
                tamanho_lote = st.selectbox("Tamanho do lote", 
                                          [500, 1000, 2000, 5000], 
                                          index=1,
                                          help="Número de linhas processadas por vez")
            else:
                tamanho_lote = 1000
                st.info("🔧 Processamento direto")
        
        with col3:
            mostrar_preview = st.checkbox("Mostrar preview", value=True)
        
        # Preview dos dados
        if mostrar_preview:
            st.subheader("👀 Preview dos Dados")
            
            preview_col1, preview_col2 = st.columns(2)
            
            with preview_col1:
                st.write("**📊 Planilha de Dados** (primeiras 5 linhas)")
                st.dataframe(data_df.head(), use_container_width=True)
            
            with preview_col2:
                st.write("**⚙️ Planilha de Configurações**")
                st.dataframe(config_df, use_container_width=True)
        
        # Processamento
        st.subheader("⚙️ Processando Dados...")
        start_time = time.time()
        
        if usar_lotes and len(data_df) > 1000:
            st.info(f"🔧 Processando em lotes de {tamanho_lote} linhas...")
            
            # Interface de progresso
            progress_bar = st.progress(0)
            status_text = st.empty()
            time_elapsed = st.empty()
            
            resultados_parciais = []
            linhas_processadas = 0
            
            for progresso, lote_processado in processar_em_lotes(data_df, config_df, tamanho_lote):
                progress_bar.progress(progresso)
                linhas_processadas = min((progresso * len(data_df)), len(data_df))
                tempo_decorrido = time.time() - start_time
                
                status_text.text(f"📈 Progresso: {progresso*100:.1f}%")
                time_elapsed.text(f"⏱️ Tempo decorrido: {tempo_decorrido:.1f}s")
                
                if progresso < 1.0:
                    resultados_parciais.append(lote_processado)
                else:
                    result_df = lote_processado
            
            status_text.text("✅ Processamento concluído!")
            time_elapsed.text(f"⏱️ Tempo total: {time.time() - start_time:.1f}s")
            
        else:
            st.info("🔧 Processamento direto...")
            result_df = processamento_direto(data_df, config_df)
        
        processing_time = time.time() - start_time
        
        # Resultado final
        st.subheader("📊 Resultado Final")
        
        # Mostrar amostra se for muito grande
        if len(result_df) > 1000:
            st.warning(f"📋 Mostrando as primeiras 1000 linhas de {len(result_df):,} totais")
            st.dataframe(result_df.head(1000), use_container_width=True)
        else:
            st.dataframe(result_df, use_container_width=True)
        
        # Estatísticas finais
        st.subheader("📈 Estatísticas do Processamento")
        
        stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
        
        with stat_col1:
            total_matches = 0
            for attr in config_df['Atributo'].unique():
                if attr in result_df.columns:
                    total_matches += (result_df[attr].str.count(',') + 1).where(result_df[attr] != '', 0).sum()
            st.metric("Total de Correspondências", f"{int(total_matches):,}")
        
        with stat_col2:
            atributos_com_match = sum([1 for attr in config_df['Atributo'].unique() 
                                     if attr in result_df.columns and result_df[attr].ne('').any()])
            st.metric("Atributos com Match", atributos_com_match)
        
        with stat_col3:
            if all(attr in result_df.columns for attr in config_df['Atributo'].unique()):
                linhas_com_match = result_df[config_df['Atributo'].unique()].ne('').any(axis=1).sum()
                st.metric("Linhas com Match", f"{linhas_com_match:,}")
            else:
                st.metric("Linhas com Match", "N/A")
        
        with stat_col4:
            st.metric("Tempo de Processamento", f"{processing_time:.1f}s")
        
        # Download do resultado
        st.subheader("📥 Download do Resultado")
        
        if len(result_df) > 50000:
            st.warning("💡 Arquivo muito grande. Recomendamos dividir o download:")
            
            partes = (len(result_df) // 50000) + 1
            for i in range(partes):
                inicio = i * 50000
                fim = min((i + 1) * 50000, len(result_df))
                parte_df = result_df.iloc[inicio:fim]
                
                parte_excel = to_excel(parte_df)
                st.download_button(
                    f"💾 Baixar Parte {i+1} (linhas {inicio+1}-{fim})", 
                    parte_excel, 
                    file_name=f"relatorio_parte_{i+1}.xlsx",
                    help=f"Parte {i+1} do relatório"
                )
        else:
            result_excel = to_excel(result_df)
            
            st.download_button(
                "💾 Baixar Relatório Completo", 
                result_excel, 
                file_name="relatorio_processado.xlsx",
                help="Planilha com os resultados do processamento",
                type="primary"
            )
        
        st.success(f"✅ Processamento concluído com sucesso em {processing_time:.1f} segundos!")
        
    except Exception as e:
        st.error(f"❌ Erro durante o processamento: {str(e)}")
        st.info("💡 Para arquivos muito grandes, tente dividi-los em partes menores.")

else:
    st.info("👆 Faça o upload de ambas as planilhas para iniciar o processamento.")

# ==================================================
# SEÇÕES INFORMATIVAS
# ==================================================

with st.expander("🚀 Estratégias para Arquivos MUITO Grandes (500MB+)"):
    st.markdown("""
    ### 📏 Se seus arquivos forem maiores que 500MB:
    
    **1. Divida os arquivos de dados:**
    - Separe em múltiplos arquivos de ~100MB cada
    - Processe um por um
    - Combine os resultados depois
    
    **2. Use um servidor com mais recursos:**
    - Aumente a memória RAM disponível
    - Use instâncias com melhor processamento
    
    **3. Otimize suas planilhas:**
    - Remova colunas desnecessárias
    - Use compactação ZIP nos arquivos XLSX
    - Converta para CSV (menor tamanho)
    
    **4. Processamento em nuvem:**
    - Use serviços como AWS, Google Cloud
    - Processe em máquinas mais potentes
    """)

with st.expander("💡 Como Funciona o Reconhecimento de Padrões"):
    st.markdown("""
    ### 🔍 Exemplo de Funcionamento:
    
    **Descrição do produto:**
    ```
    "Ventilador de teto 110 amarelo biv"
    ```
    
    **Configuração de Voltagem:**
    - Padrão: `110,110v,127`
    - Variação: `110v`
    
    **Resultado:** A coluna "Voltagem" será preenchida com `110v`
    
    ### ⚠️ Regras do Reconhecimento:
    - Busca por **palavras completas** (usando `\\b` no regex)
    - **Case insensitive** (não diferencia maiúsculas/minúsculas)
    - **Múltiplos padrões** separados por vírgula
    - **Primeira correspondência** prevalece
    """)

with st.expander("📋 Estrutura dos Arquivos"):
    st.markdown("""
    ### 📊 Planilha de Dados:
    | ID | Descrição |
    |----|-----------|
    | 1414 | Ventilador de teto 110 amarelo biv |
    | 2525 | Luminária LED 220v branca |
    
    ### ⚙️ Planilha de Configurações:
    | Atributo | Variação | Padrão de reconhecimento |
    |----------|----------|--------------------------|
    | Voltagem | 110v | 110,110v,127 |
    | Voltagem | 220v | 220,220v,227 |
    | Cor | Amarelo | amarelo,yellow |
    """)

with st.expander("⚙️ Configurações Técnicas"):
    st.markdown(f"""
    ### 🛠️ Configurações do Sistema:
    - **Limite Render:** {MAX_ROWS_RENDER:,} linhas
    - **Processamento em lotes:** Ativado automaticamente > 5.000 linhas
    - **Tamanho máximo de lote:** 5.000 linhas
    - **Formato suportado:** XLSX
    
    ### 📊 Performance Esperada:
    - **Até 10.000 linhas:** 10-30 segundos
    - **10.000-50.000 linhas:** 1-5 minutos  
    - **50.000+ linhas:** 5+ minutos (depende do hardware)
    """)

# ==================================================
# RODAPÉ
# ==================================================

st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666;'>
    <p>Desenvolvido com Streamlit • Otimizado para grandes arquivos • Versão 2.0</p>
</div>
""", unsafe_allow_html=True)

# Limpeza de cache para produção
if is_render():
    try:
        if hasattr(st, 'cache_data'):
            st.cache_data.clear()
        if hasattr(st, 'cache_resource'):
            st.cache_resource.clear()
    except:
        pass
