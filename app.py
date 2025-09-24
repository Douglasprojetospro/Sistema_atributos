import streamlit as st
import pandas as pd
from io import BytesIO
import re
import tempfile
import os
import time

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

# Upload de arquivos
st.subheader("📤 Upload dos Arquivos")

upload_col1, upload_col2 = st.columns(2)

with upload_col1:
    st.info("💡 **Dica:** Para arquivos grandes, use as estratégias abaixo")
    data_file = st.file_uploader("Planilha de dados (XLSX)", type="xlsx", key="data_upload")

with upload_col2:
    st.info("💡 **Arquivo de configurações:** Geralmente é pequeno")
    config_file = st.file_uploader("Planilha de configurações (XLSX)", type="xlsx", key="config_upload")

# Função para processamento básico
def processar_linha(descricao, config_groups):
    """Processa uma linha individual"""
    resultado = {}
    descricao = str(descricao).lower()
    
    for attr, group in config_groups:
        matched_variations = []
        
        for _, config_row in group.iterrows():
            patterns = [p.strip().lower() for p in str(config_row['Padrão de reconhecimento']).split(',')]
            variation = str(config_row['Variação'])
            
            for pattern in patterns:
                if pattern and re.search(r'\b' + re.escape(pattern) + r'\b', descricao):
                    if variation not in matched_variations:
                        matched_variations.append(variation)
                    break
        
        resultado[attr] = ', '.join(matched_variations) if matched_variations else ''
    
    return resultado

# Função para processamento em partes
def processar_em_partes(data_df, config_df, num_partes=2):
    """Processa o arquivo dividindo em partes"""
    resultados = []
    total_linhas = len(data_df)
    linhas_por_parte = total_linhas // num_partes
    
    # Pré-processar configurações
    config_groups = list(config_df.groupby('Atributo'))
    
    for parte in range(num_partes):
        st.subheader(f"🔄 Parte {parte + 1} de {num_partes}")
        
        # Calcular índices
        inicio = parte * linhas_por_parte
        if parte == num_partes - 1:
            fim = total_linhas
        else:
            fim = (parte + 1) * linhas_por_parte
        
        parte_df = data_df.iloc[inicio:fim].copy()
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Processar cada linha da parte
        for i, (idx, linha) in enumerate(parte_df.iterrows()):
            if i % 100 == 0:  # Atualizar progresso a cada 100 linhas
                progresso = i / len(parte_df)
                progress_bar.progress(progresso)
                status_text.text(f"Processando linha {i+1} de {len(parte_df)}")
            
            resultado = processar_linha(linha['Descrição'], config_groups)
            for attr, valor in resultado.items():
                parte_df.at[idx, attr] = valor
        
        progress_bar.progress(1.0)
        status_text.text(f"✅ Parte {parte + 1} concluída")
        resultados.append(parte_df)
        
        # Pequena pausa para evitar timeout
        time.sleep(1)
    
    # Combinar resultados
    result_df = pd.concat(resultados, ignore_index=True)
    return result_df

# Função para processamento rápido (até 10k linhas)
def processamento_rapido(data_df, config_df):
    """Processamento otimizado para arquivos menores"""
    result_df = data_df.copy()
    config_groups = config_df.groupby('Atributo')
    
    progress_bar = st.progress(0)
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
    return result_df

if data_file and config_file:
    try:
        # Ler arquivos
        st.subheader("📖 Lendo Arquivos...")
        
        config_df = pd.read_excel(config_file)
        data_df = pd.read_excel(data_file)
        
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
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Linhas de dados", len(data_df))
        
        with col2:
            st.metric("Atributos configurados", len(config_df['Atributo'].unique()))
        
        with col3:
            tamanho_memoria = data_df.memory_usage(deep=True).sum() / (1024 * 1024)
            st.metric("Uso de memória (MB)", f"{tamanho_memoria:.1f}")
        
        # Seleção de estratégia de processamento
        st.subheader("⚙️ Estratégia de Processamento")
        
        total_linhas = len(data_df)
        
        if total_linhas <= 10000:
            st.success("✅ Arquivo de tamanho ideal para processamento rápido")
            estrategia = "rápido"
            num_partes = 1
            
        elif total_linhas <= 50000:
            st.warning("⚠️ Arquivo grande - Usando processamento em partes")
            estrategia = "partes"
            num_partes = min((total_linhas // 10000) + 1, 5)
            st.info(f"Será dividido em {num_partes} partes")
            
        else:
            st.error("🚨 Arquivo muito grande - Estratégia avançada necessária")
            estrategia = "avançado"
            num_partes = min((total_linhas // 10000) + 1, 10)
            st.info(f"Recomendado: dividir manualmente em arquivos menores ou usar {num_partes} partes")
        
        # Configurações adicionais
        if estrategia in ["partes", "avançado"]:
            num_partes = st.slider(
                "Número de partes para divisão:", 
                min_value=2, 
                max_value=10, 
                value=num_partes,
                help="Mais partes = menor uso de memória, mas mais tempo de processamento"
            )
        
        # Processamento
        st.subheader("⚙️ Processando Dados...")
        
        start_time = time.time()
        
        if estrategia == "rápido":
            result_df = processamento_rapido(data_df, config_df)
        else:
            result_df = processar_em_partes(data_df, config_df, num_partes)
        
        processing_time = time.time() - start_time
        
        # Resultados
        st.subheader("📊 Resultado Final")
        st.dataframe(result_df.head(1000), use_container_width=True)
        
        if len(result_df) > 1000:
            st.info(f"📋 Mostrando 1000 de {len(result_df)} linhas totais")
        
        # Estatísticas
        st.subheader("📈 Estatísticas do Processamento")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_matches = sum([result_df[attr].str.count(',').sum() + result_df[attr].ne('').sum() 
                               for attr in config_df['Atributo'].unique()])
            st.metric("Total de Correspondências", int(total_matches))
        
        with col2:
            linhas_com_match = result_df[[attr for attr in config_df['Atributo'].unique()]].ne('').any(axis=1).sum()
            st.metric("Linhas com Match", linhas_com_match)
        
        with col3:
            st.metric("Tempo de Processamento", f"{processing_time:.1f}s")
        
        # Download
        st.subheader("📥 Download do Resultado")
        
        if len(result_df) <= 50000:
            # Download único
            result_excel = to_excel(result_df)
            st.download_button(
                "💾 Baixar Relatório Completo", 
                result_excel, 
                file_name="relatorio_final.xlsx",
                type="primary"
            )
        else:
            # Download em partes
            st.warning("📁 Arquivo muito grande para download único")
            partes_download = (len(result_df) // 50000) + 1
            
            for i in range(partes_download):
                inicio = i * 50000
                fim = min((i + 1) * 50000, len(result_df))
                parte_df = result_df.iloc[inicio:fim]
                
                parte_excel = to_excel(parte_df)
                st.download_button(
                    f"📥 Baixar Parte {i+1} (linhas {inicio+1}-{fim})", 
                    parte_excel, 
                    file_name=f"relatorio_parte_{i+1}.xlsx"
                )
        
        st.success(f"✅ Processamento concluído em {processing_time:.1f} segundos!")
        
    except Exception as e:
        st.error(f"❌ Erro durante o processamento: {str(e)}")
        
        if is_render() and len(data_df) > 50000:
            st.info("""
            💡 **Dica para arquivos muito grandes no Render:**
            - Divida manualmente seu arquivo em partes de ~10.000 linhas
            - Processe uma parte por vez
            - Combine os resultados manualmente no Excel
            """)

else:
    st.info("👆 Faça o upload de ambas as planilhas para iniciar o processamento.")

# Guia de estratégias
with st.expander("🚀 GUIA: Como Processar Arquivos Grandes"):
    st.markdown("""
    ### 📏 Tamanho do Arquivo vs Estratégia:
    
    **✅ Até 10.000 linhas:** Processamento rápido
    - Tempo estimado: 1-30 segundos
    - Estratégia: Automática
    
    **⚠️ 10.000-50.000 linhas:** Processamento em partes  
    - Tempo estimado: 30 segundos - 5 minutos
    - Estratégia: Divisão automática
    
    **🚨 50.000+ linhas:** Estratégia avançada
    - Tempo estimado: 5+ minutos (pode ter timeout)
    - Estratégia: Divisão manual recomendada
    
    ### 💡 Dicas para Arquivos MUITO Grandes:
    1. **Divida manualmente** em arquivos de ~10.000 linhas
    2. **Processe um por um**
    3. **Combine os resultados** no Excel
    4. **Considere upgrade** para plano pago no Render
    """)

with st.expander("ℹ️ Instruções de Uso"):
    st.markdown("""
    ### Como usar:
    1. **Baixe os modelos** acima
    2. **Preencha as planilhas** com seus dados
    3. **Faça o upload** das duas planilhas  
    4. **Aguarde o processamento** (estratégia automática)
    5. **Baixe o resultado**
    
    ### Exemplo de Funcionamento:
    - **Descrição**: "Ventilador de teto 110 amarelo biv"
    - **Configuração**: Voltagem 110v → padrões: "110,110v,127"
    - **Resultado**: Coluna "Voltagem" com valor "110v"
    """)

# Finalização para Render
if __name__ == "__main__":
    # Limpeza de cache para versões novas do Streamlit
    if is_render():
        try:
            if hasattr(st, 'cache_data'):
                st.cache_data.clear()
            if hasattr(st, 'cache_resource'):
                st.cache_resource.clear()
        except:
            pass
