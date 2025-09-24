import streamlit as st
import pandas as pd
from io import BytesIO
import re
import tempfile
import os

# Configurações específicas para deploy no Render
st.set_page_config(
    page_title="Processador de Planilhas - Otimizado", 
    page_icon="📊", 
    layout="wide"
)

# Configurações do pandas para melhor performance
pd.set_option('mode.chained_assignment', None)

# Verifica se está em produção no Render
def is_render():
    return 'RENDER' in os.environ or ('HOSTNAME' in os.environ and 'render' in os.environ['HOSTNAME'])

# Aplicar estilos específicos para produção
if is_render():
    st.markdown("""
    <style>
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    </style>
    """, unsafe_allow_html=True)

st.title("📊 Processador de Planilhas - Otimizado para Grandes Arquivos")
st.markdown("---")

# Colunas para os templates
col1, col2 = st.columns(2)

with col1:
    st.subheader("📋 Modelos para Download")
    
    # Template de dados
    data_template = pd.DataFrame({
        'ID': [1414, 2525],
        'Descrição': ['Ventilador de teto 110 amarelo biv', 'Luminária LED 220v branca']
    })
    
    # Template de configurações
    config_template = pd.DataFrame({
        'Atributo': ['Voltagem', 'Voltagem', 'Voltagem', 'Cor', 'Cor'],
        'Variação': ['110v', '220v', 'Bivolt', 'Amarelo', 'Branca'],
        'Padrão de reconhecimento': ['110,110v,127', '220,220v,227', 'bivolt,biv', 'amarelo,yellow', 'branca,white']
    })

    # Função para converter DataFrame para Excel
    def to_excel(df):
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
        return output.getvalue()

    st.download_button(
        "📥 Baixar modelo de dados", 
        to_excel(data_template), 
        file_name="modelo_dados.xlsx",
        help="Modelo da planilha com colunas ID e Descrição"
    )
    
    st.download_button(
        "📥 Baixar modelo de configurações", 
        to_excel(config_template), 
        file_name="modelo_config.xlsx",
        help="Modelo da planilha com colunas Atributo, Variação e Padrão de reconhecimento"
    )

st.markdown("---")

# Upload de arquivos com opções para grandes arquivos
st.subheader("📤 Upload dos Arquivos (Otimizado para Grandes Arquivos)")

upload_col1, upload_col2 = st.columns(2)

with upload_col1:
    st.info("💡 **Dica para arquivos grandes:** Divida sua planilha de dados em partes menores se possível.")
    data_file = st.file_uploader("Planilha de dados (XLSX)", type="xlsx", key="data_upload")

with upload_col2:
    st.info("💡 **Arquivo de configurações:** Geralmente é pequeno, sem problemas de tamanho.")
    config_file = st.file_uploader("Planilha de configurações (XLSX)", type="xlsx", key="config_upload")

# Função otimizada para processamento em lotes
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
            patterns = [p.strip().lower() for p in str(row['Padrão de reconhecimento']).split(',')]
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
    
    yield 1.0, pd.concat(resultados, ignore_index=True) if resultados else pd.DataFrame()

# Função para ler arquivo de forma eficiente
def ler_arquivo_eficiente(arquivo):
    """
    Lê arquivo Excel de forma otimizada
    """
    try:
        # Para o Render, usar abordagem mais simples
        if arquivo.name.endswith('.xlsx'):
            df = pd.read_excel(arquivo, engine='openpyxl')
            return df
            
    except Exception as e:
        st.error(f"Erro ao ler arquivo: {str(e)}")
        return None

if data_file and config_file:
    try:
        # Mostrar informações de processamento
        st.subheader("⚙️ Configurações de Processamento")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # No Render Free, usar lotes menores para evitar timeout
            tamanho_lote_render = 500 if is_render() else 1000
            usar_lotes = st.checkbox("Usar processamento em lotes", value=True, 
                                   help="Recomendado para arquivos grandes")
        
        with col2:
            tamanho_lote = st.selectbox("Tamanho do lote", 
                                      [500, 1000, 2000, 5000], 
                                      index=0 if is_render() else 1,
                                      help="Número de linhas processadas por vez")
        
        with col3:
            mostrar_preview = st.checkbox("Mostrar preview", value=True,
                                        help="Mostrar amostra dos dados")
        
        # Ler arquivos
        st.subheader("📖 Lendo Arquivos...")
        
        progress_bar_leit = st.progress(0)
        
        # Ler arquivo de configurações (geralmente pequeno)
        config_df = pd.read_excel(config_file)
        progress_bar_leit.progress(50)
        
        # Ler arquivo de dados
        data_df = ler_arquivo_eficiente(data_file)
        progress_bar_leit.progress(100)
        
        if data_df is None:
            st.error("❌ Erro ao ler arquivo de dados")
            st.stop()
        
        # Validar colunas
        required_data_cols = ['ID', 'Descrição']
        required_config_cols = ['Atributo', 'Variação', 'Padrão de reconhecimento']
        
        if not all(col in data_df.columns for col in required_data_cols):
            st.error(f"❌ Planilha de dados deve conter as colunas: {required_data_cols}")
            st.stop()
            
        if not all(col in config_df.columns for col in required_config_cols):
            st.error(f"❌ Planilha de configurações deve conter as colunas: {required_config_cols}")
            st.stop()
        
        # Mostrar estatísticas
        st.subheader("📊 Estatísticas dos Dados")
        info_col1, info_col2, info_col3, info_col4 = st.columns(4)
        
        with info_col1:
            st.metric("Linhas de dados", len(data_df))
        
        with info_col2:
            st.metric("Colunas de dados", len(data_df.columns))
        
        with info_col3:
            st.metric("Atributos configurados", len(config_df['Atributo'].unique()))
        
        with info_col4:
            tamanho_memoria = data_df.memory_usage(deep=True).sum() / (1024 * 1024)
            st.metric("Uso de memória (MB)", f"{tamanho_memoria:.1f}")
        
        # Aviso para Render Free
        if is_render() and len(data_df) > 10000:
            st.warning("⚠️ **Aviso Render Free:** Arquivos muito grandes podem causar timeout. Recomendamos processar até 10.000 linhas por vez.")
        
        if mostrar_preview:
            st.subheader("👀 Preview dos Dados")
            
            preview_col1, preview_col2 = st.columns(2)
            
            with preview_col1:
                st.write("**Planilha de Dados** (primeiras 5 linhas)")
                st.dataframe(data_df.head(), use_container_width=True)
            
            with preview_col2:
                st.write("**Planilha de Configurações**")
                st.dataframe(config_df.head(10), use_container_width=True)
        
        # Processamento
        st.subheader("⚙️ Processando Dados...")
        
        if usar_lotes and len(data_df) > 1000:
            st.info(f"🔧 Processando em lotes de {tamanho_lote} linhas...")
            
            # Criar placeholder para resultados parciais
            resultado_placeholder = st.empty()
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            resultados_parciais = []
            
            for progresso, lote_processado in processar_em_lotes(data_df, config_df, tamanho_lote):
                progress_bar.progress(progresso)
                status_text.text(f"Progresso: {progresso*100:.1f}% - Processadas {min((progresso * len(data_df)), len(data_df)):.0f} de {len(data_df)} linhas")
                
                if progresso < 1.0:
                    resultados_parciais.append(lote_processado)
                else:
                    result_df = lote_processado  # Último yield contém o DataFrame completo
            
            status_text.text("✅ Processamento concluído!")
            
        else:
            # Processamento direto para arquivos menores
            st.info("🔧 Processamento direto (arquivo pequeno)...")
            progress_bar = st.progress(0)
            
            # Processar tudo de uma vez
            config_groups = config_df.groupby('Atributo')
            result_df = data_df.copy()
            
            total_attrs = len(config_groups)
            
            for i, (attr, group) in enumerate(config_groups):
                progress_bar.progress(i / total_attrs)
                
                variations_list = []
                for _, data_row in data_df.iterrows():
                    descricao = str(data_row['Descrição']).lower()
                    matched_variations = []
                    
                    for _, config_row in group.iterrows():
                        patterns = [p.strip().lower() for p in str(config_row['Padrão de reconhecimento']).split(',')]
                        variation = str(config_row['Variação'])
                        
                        for pattern in patterns:
                            if pattern and re.search(r'\b' + re.escape(pattern) + r'\b', descricao):
                                if variation not in matched_variations:
                                    matched_variations.append(variation)
                                break
                    
                    variations_list.append(', '.join(matched_variations) if matched_variations else '')
                
                result_df[attr] = variations_list
            
            progress_bar.progress(1.0)
        
        # Resultado final
        st.subheader("📊 Resultado Final")
        
        # Mostrar apenas uma amostra se for muito grande
        if len(result_df) > 1000:
            st.warning(f"📋 Mostrando as primeiras 1000 linhas de {len(result_df)} total")
            st.dataframe(result_df.head(1000), use_container_width=True)
        else:
            st.dataframe(result_df, use_container_width=True)
        
        # Estatísticas finais
        st.subheader("📈 Estatísticas do Processamento")
        stat_col1, stat_col2, stat_col3 = st.columns(3)
        
        with stat_col1:
            total_matches = sum([result_df[attr].str.count(',').sum() + result_df[attr].ne('').sum() 
                               for attr in config_df['Atributo'].unique()])
            st.metric("Total de Correspondências", int(total_matches))
        
        with stat_col2:
            atributos_com_match = sum([1 for attr in config_df['Atributo'].unique() 
                                     if result_df[attr].ne('').any()])
            st.metric("Atributos com Match", atributos_com_match)
        
        with stat_col3:
            linhas_com_match = result_df[[attr for attr in config_df['Atributo'].unique()]].ne('').any(axis=1).sum()
            st.metric("Linhas com Match", linhas_com_match)
        
        # Download do resultado
        st.subheader("📥 Download do Resultado")
        
        # Aviso para Render Free
        if is_render() and len(result_df) > 50000:
            st.error("📁 **Arquivo muito grande para Render Free:** Recomendamos dividir o processamento.")
        else:
            result_excel = to_excel(result_df)
            
            st.download_button(
                "💾 Baixar Relatório Completo", 
                result_excel, 
                file_name="relatorio_final.xlsx",
                help="Planilha com os resultados do processamento",
                type="primary"
            )
        
        st.success("✅ Processamento concluído com sucesso!")
        
    except Exception as e:
        st.error(f"❌ Erro durante o processamento: {str(e)}")
        if is_render():
            st.info("💡 **Dica Render:** Para arquivos grandes, tente dividir em partes menores.")
        else:
            st.info("💡 Para arquivos muito grandes, tente dividi-los em partes menores.")

else:
    st.info("👆 Faça o upload de ambas as planilhas para iniciar o processamento.")

# Estratégias para arquivos muito grandes
with st.expander("🚀 Estratégias para Arquivos Grandes no Render"):
    st.markdown("""
    ### No Render Free:
    
    **Limitações:**
    - 512MB RAM
    - Timeout após alguns minutos
    - Aplicação dorme após inatividade
    
    **Recomendações:**
    - Processe até 10.000 linhas por vez
    - Divida arquivos grandes em partes
    - Use tamanho de lote de 500 linhas
    
    ### Para melhor performance:
    
    1. **Divida os arquivos de dados:**
    - Separe em múltiplos arquivos de ~5.000 linhas cada
    - Processe um por um
    
    2. **Otimize as configurações:**
    - Use padrões de busca mais específicos
    - Remova colunas desnecessárias das planilhas
    """)

with st.expander("ℹ️ Instruções de Uso"):
    st.markdown("""
    ### Como usar:
    1. **Baixe os modelos** acima
    2. **Preencha as planilhas** seguindo os modelos
    3. **Faça o upload** das duas planilhas
    4. **Aguarde o processamento** e baixe o resultado
    
    ### Exemplo:
    - **Descrição**: "Ventilador de teto 110 amarelo biv"
    - **Configuração Voltagem**: 
      - 110v → padrões: "110,110v,127"
      - Bivolt → padrões: "bivolt,biv"
    - **Resultado**: Coluna "Voltagem" com valor "110v, Bivolt"
    """)

# Esta parte final é importante para o Render
if __name__ == "__main__":
    # Configurações adicionais para produção
    if is_render():
        st.runtime.legacy_caching.clear_cache()
