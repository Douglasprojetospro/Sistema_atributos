import streamlit as st
import pandas as pd
from io import BytesIO
import re
import tempfile
import os
from pathlib import Path
import time

# ==================================================
# CONFIGURAÇÕES INICIAIS E DETECÇÃO DE AMBIENTE
# ==================================================

def is_render():
    """Detecta se está executando no ambiente Render"""
    return 'RENDER' in os.environ or ('HOSTNAME' in os.environ and 'render' in os.environ['HOSTNAME'])

def get_render_plan():
    """Tenta detectar o tipo de instância do Render"""
    if not is_render():
        return "local"
    
    # Verificar variáveis de ambiente que podem indicar plano
    if 'RENDER_INSTANCE_TYPE' in os.environ:
        instance_type = os.environ['RENDER_INSTANCE_TYPE']
        if 'starter' in instance_type.lower() or 'standard' in instance_type.lower() or 'paid' in instance_type.lower():
            return "paid"
    
    # Verificar recursos disponíveis (abordagem heurística)
    try:
        import psutil
        ram_gb = psutil.virtual_memory().total / (1024 ** 3)
        if ram_gb > 1.0:  # Plano free tem ~512MB, paid tem mais
            return "paid"
    except:
        pass
    
    # Por padrão, assumir free se não conseguir detectar
    return "free"

# Configurações da página
st.set_page_config(
    page_title="Processador de Planilhas - Premium", 
    page_icon="🚀", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configurações do pandas para melhor performance
pd.set_option('mode.chained_assignment', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)

# ==================================================
# CONFIGURAÇÕES OTIMIZADAS PARA PLANO PAGO
# ==================================================

render_plan = get_render_plan()

# Limites dinâmicos baseados no plano
if is_render():
    if render_plan == "paid":
        # LIMITES AMPLIADOS PARA PLANO PAGO
        AVISO_LIMITE_RENDER = 100000  # Aviso a partir de 100k linhas
        LIMITE_CRITICO_RENDER = 200000  # Limite crítico para 200k linhas
        MAX_LINHAS_RECOMENDADO = 150000
        TAMANHO_LOTE_OTIMO = 2000
        TIMEOUT_PROCESSAMENTO = 600  # 10 minutos
    else:
        # Limites conservadores para plano free
        AVISO_LIMITE_RENDER = 50000
        LIMITE_CRITICO_RENDER = 80000
        MAX_LINHAS_RECOMENDADO = 50000
        TAMANHO_LOTE_OTIMO = 500
        TIMEOUT_PROCESSAMENTO = 300  # 5 minutos
else:
    # Limites para execução local
    AVISO_LIMITE_RENDER = 150000
    LIMITE_CRITICO_RENDER = 300000
    MAX_LINHAS_RECOMENDADO = 200000
    TAMANHO_LOTE_OTIMO = 3000
    TIMEOUT_PROCESSAMENTO = 900  # 15 minutos

# Aplicar estilos específicos
if is_render():
    st.markdown("""
    <style>
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    .plano-paid {
        background-color: #e8f5e8;
        padding: 10px;
        border-radius: 5px;
        border-left: 4px solid #28a745;
    }
    .plano-free {
        background-color: #fff3cd;
        padding: 10px;
        border-radius: 5px;
        border-left: 4px solid #ffc107;
    }
    </style>
    """, unsafe_allow_html=True)

# ==================================================
# FUNÇÕES AUXILIARES OTIMIZADAS
# ==================================================

def to_excel(df):
    """
    Converte DataFrame para Excel em memória com otimizações
    """
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Resultado')
        
        # Otimizações para grandes arquivos
        workbook = writer.book
        worksheet = writer.sheets['Resultado']
        
        # Configurar para melhor performance
        worksheet.set_default_row(hide_unused_rows=True)
        
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
        
        # Estratégias diferentes baseadas no tamanho e plano
        if tamanho_mb > 100:  # Arquivo muito grande
            st.warning("⚡ Arquivo grande detectado. Usando modo de leitura otimizado...")
            
            # Ler metadados primeiro
            xl = pd.ExcelFile(temp_path)
            primeira_linha = pd.read_excel(temp_path, nrows=1)
            
            # Verificar colunas necessárias
            colunas_necessarias = ['ID', 'Descrição']
            colunas_disponiveis = [col for col in colunas_necessarias if col in primeira_linha.columns]
            
            if len(colunas_disponiveis) == len(colunas_necessarias):
                # Ler apenas colunas necessárias com tipos otimizados
                df = pd.read_excel(
                    temp_path,
                    usecols=colunas_necessarias,
                    dtype={'ID': 'string', 'Descrição': 'string'},
                    engine='openpyxl'
                )
            else:
                # Ler todas as colunas com otimização de tipos
                df = pd.read_excel(temp_path, engine='openpyxl')
                # Otimizar tipos de dados
                for col in df.columns:
                    if df[col].dtype == 'object':
                        df[col] = df[col].astype('string')
                
        else:
            # Leitura normal com otimizações
            df = pd.read_excel(temp_path, engine='openpyxl')
            # Otimizar tipos de dados
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype('string')
        
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

def processar_em_lotes_otimizado(data_df, config_df, tamanho_lote=2000):
    """
    Processa os dados em lotes com otimizações para plano pago
    """
    # Pré-processar configurações de forma mais eficiente
    config_dict = {}
    for attr, group in config_df.groupby('Atributo'):
        config_dict[attr] = []
        for _, row in group.iterrows():
            patterns = [p.strip().lower() for p in str(row['Padrão de reconhecimento']).split(',') if p.strip()]
            config_dict[attr].append({
                'variation': str(row['Variação']),
                'patterns': patterns
            })
    
    # Compilar regex patterns uma única vez
    for attr, configs in config_dict.items():
        for config in configs:
            config['compiled_patterns'] = [re.compile(r'\b' + re.escape(pattern) + r'\b') for pattern in config['patterns']]
    
    # Processar em lotes otimizados
    resultados = []
    total_linhas = len(data_df)
    start_time = time.time()
    
    for i in range(0, total_linhas, tamanho_lote):
        # Verificar timeout
        if time.time() - start_time > TIMEOUT_PROCESSAMENTO:
            st.error("⏰ Timeout de processamento atingido")
            break
            
        lote = data_df.iloc[i:i + tamanho_lote].copy()
        
        for attr, configs in config_dict.items():
            coluna_resultado = []
            
            for _, linha in lote.iterrows():
                descricao = str(linha['Descrição']).lower()
                variacoes_encontradas = []
                
                for config in configs:
                    for compiled_pattern in config['compiled_patterns']:
                        if compiled_pattern.search(descricao):
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

def processamento_direto_otimizado(data_df, config_df):
    """
    Processamento direto otimizado para plano pago
    """
    # Pré-compilar patterns para melhor performance
    config_groups = config_df.groupby('Atributo')
    config_dict = {}
    
    for attr, group in config_groups:
        config_dict[attr] = []
        for _, row in group.iterrows():
            patterns = [p.strip().lower() for p in str(row['Padrão de reconhecimento']).split(',') if p.strip()]
            compiled_patterns = [re.compile(r'\b' + re.escape(pattern) + r'\b') for pattern in patterns]
            config_dict[attr].append({
                'variation': str(row['Variação']),
                'compiled_patterns': compiled_patterns
            })
    
    result_df = data_df.copy()
    total_attrs = len(config_dict)
    
    for i, (attr, configs) in enumerate(config_dict.items()):
        variations_list = []
        
        # Processar em chunks menores mesmo no modo direto
        chunk_size = 1000
        for j in range(0, len(data_df), chunk_size):
            chunk = data_df.iloc[j:j + chunk_size]
            
            for _, data_row in chunk.iterrows():
                descricao = str(data_row['Descrição']).lower()
                matched_variations = []
                
                for config in configs:
                    for compiled_pattern in config['compiled_patterns']:
                        if compiled_pattern.search(descricao):
                            if config['variation'] not in matched_variations:
                                matched_variations.append(config['variation'])
                            break
                
                variations_list.append(', '.join(matched_variations) if matched_variations else '')
        
        result_df[attr] = variations_list
    
    return result_df

# ==================================================
# INTERFACE DO USUÁRIO PREMIUM
# ==================================================

st.title("🚀 Processador de Planilhas - Versão Premium")
st.markdown("""
*Processamento otimizado para grandes volumes de dados com máxima performance*
""")

# ==================================================
# BANNER DE STATUS DO PLANO
# ==================================================

if is_render():
    if render_plan == "paid":
        st.markdown(f"""
        <div class="plano-paid">
            <h3>🚀 Modo Premium Ativado</h3>
            <p><strong>Plano Pago Detectado</strong> - Recursos ampliados disponíveis!</p>
            <p>✅ Limite: <strong>{LIMITE_CRITICO_RENDER:,} linhas</strong> | ⚡ Lote ótimo: <strong>{TAMANHO_LOTE_OTIMO} linhas</strong></p>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div class="plano-free">
            <h3>💡 Potencialize seu Processamento</h3>
            <p><strong>Plano Free Detectado</strong> - Atualize para desbloquear recursos premium!</p>
            <p>⚡ Com plano pago: <strong>Até {LIMITE_CRITICO_RENDER:,} linhas</strong> | 🕒 Processamento 3x mais rápido</p>
            <p><a href="https://render.com/pricing" target="_blank">🔗 Saiba mais sobre planos pagos</a></p>
        </div>
        """, unsafe_allow_html=True)

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
# SEÇÃO DE UPLOAD INTELIGENTE
# ==================================================

st.subheader("📤 Upload dos Arquivos")

upload_col1, upload_col2 = st.columns(2)

with upload_col1:
    st.info("""
    **📊 Planilha de Dados:**
    - Colunas obrigatórias: **ID** e **Descrição**
    - **Limites recomendados:**
      - Plano Free: até 50.000 linhas
      - Plano Pago: até 150.000 linhas
    """)
    data_file = st.file_uploader("Planilha de dados", type="xlsx", key="data_upload")

with upload_col2:
    st.info("""
    **⚙️ Planilha de Configurações:**
    - Colunas obrigatórias: **Atributo**, **Variação**, **Padrão de reconhecimento**
    - Padrões separados por vírgula
    - Geralmente é um arquivo pequeno
    """)
    config_file = st.file_uploader("Planilha de configurações", type="xlsx", key="config_upload")

# ==================================================
# PROCESSAMENTO PRINCIPAL PREMIUM
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
            total_linhas = len(data_df)
            st.metric("Linhas de dados", f"{total_linhas:,}")
        
        with info_col2:
            st.metric("Colunas de dados", len(data_df.columns))
        
        with info_col3:
            atributos_unicos = len(config_df['Atributo'].unique())
            st.metric("Atributos configurados", atributos_unicos)
        
        with info_col4:
            tamanho_memoria = data_df.memory_usage(deep=True).sum() / (1024 * 1024)
            st.metric("Uso de memória (MB)", f"{tamanho_memoria:.1f}")
        
        # ==================================================
        # SISTEMA INTELIGENTE DE RECOMENDAÇÕES
        # ==================================================
        
        st.subheader("⚙️ Configurações de Processamento")
        
        # Análise inteligente do arquivo
        if total_linhas > LIMITE_CRITICO_RENDER:
            st.error(f"🚨 **Arquivo muito grande** - {total_linhas:,} linhas")
            if is_render() and render_plan == "free":
                st.warning(f"""
                **💡 Recomendação:** Atualize para plano pago para processar arquivos acima de {AVISO_LIMITE_RENDER:,} linhas
                
                **Alternativas:**
                1. Divida o arquivo em partes menores
                2. Processe localmente com mais recursos
                3. Atualize para plano pago no Render
                """)
            else:
                st.warning("""
                **⚠️ Processamento pode ser instável**
                - Use processamento em lotes
                - Feche outras abas do navegador
                - Tenha paciência, pode demorar
                """)
            
            tentar_mesmo_assim = st.checkbox(
                "Tentar processar mesmo assim", 
                value=False,
                help="Processamento pode ser interrompido por falta de recursos"
            )
            
            if not tentar_mesmo_assim:
                st.stop()
                
        elif total_linhas > AVISO_LIMITE_RENDER:
            st.warning(f"⚠️ **Arquivo grande detectado** - {total_linhas:,} linhas")
            if is_render() and render_plan == "free":
                st.info(f"💡 **Dica:** Com plano pago você processaria até {LIMITE_CRITICO_RENDER:,} linhas com facilidade!")
        
        # Configurações de processamento adaptativas
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Configuração automática baseada no plano
            if total_linhas > 5000:
                usar_lotes = True
                st.success("🔧 **Processamento em lotes ativado**")
            else:
                usar_lotes = st.checkbox("Usar processamento em lotes", value=False)
            
            if is_render() and render_plan == "paid":
                st.info("⚡ **Modo Premium Ativo**")
        
        with col2:
            if usar_lotes:
                # Tamanho de lote otimizado para o plano
                tamanho_lote = st.selectbox(
                    "Tamanho do lote", 
                    [500, 1000, 2000, 3000, 5000], 
                    index=[500, 1000, 2000, 3000, 5000].index(TAMANHO_LOTE_OTIMO),
                    help=f"Lote ótimo para seu plano: {TAMANHO_LOTE_OTIMO} linhas"
                )
            else:
                tamanho_lote = TAMANHO_LOTE_OTIMO
                if total_linhas > 10000:
                    st.warning("💡 Recomendado usar lotes para melhor performance")
        
        with col3:
            mostrar_preview = st.checkbox("Mostrar preview", value=total_linhas <= 10000)
            if total_linhas > 10000 and mostrar_preview:
                st.info("📋 Preview limitado para arquivos grandes")
        
        # Preview dos dados
        if mostrar_preview:
            st.subheader("👀 Preview dos Dados")
            preview_col1, preview_col2 = st.columns(2)
            with preview_col1:
                st.write("**📊 Planilha de Dados** (primeiras 10 linhas)")
                st.dataframe(data_df.head(10), use_container_width=True)
            with preview_col2:
                st.write("**⚙️ Planilha de Configurações**")
                st.dataframe(config_df, use_container_width=True)
        
        # ==================================================
        # PROCESSAMENTO OTIMIZADO
        # ==================================================
        
        st.subheader("⚙️ Processando Dados...")
        start_time = time.time()
        
        # Barra de progresso principal
        progress_bar = st.progress(0)
        status_text = st.empty()
        metrics_col1, metrics_col2, metrics_col3 = st.columns(3)
        
        with metrics_col1:
            tempo_decorrido = st.empty()
        with metrics_col2:
            linhas_processadas = st.empty()
        with metrics_col3:
            velocidade = st.empty()
        
        if usar_lotes and total_linhas > 1000:
            st.info(f"🔧 Processando em lotes de {tamanho_lote} linhas...")
            
            resultados_parciais = []
            linhas_processadas_total = 0
            
            for progresso, lote_processado in processar_em_lotes_otimizado(data_df, config_df, tamanho_lote):
                progress_bar.progress(progresso)
                linhas_processadas_total = min((progresso * total_linhas), total_linhas)
                
                # Atualizar métricas em tempo real
                tempo_decorrido_sec = time.time() - start_time
                tempo_decorrido.metric("⏱️ Tempo", f"{tempo_decorrido_sec:.1f}s")
                linhas_processadas.metric("📈 Linhas", f"{linhas_processadas_total:,}")
                
                if tempo_decorrido_sec > 0:
                    velo_sec = linhas_processadas_total / tempo_decorrido_sec
                    velocidade.metric("⚡ Velocidade", f"{velo_sec:.0f} linhas/s")
                
                status_text.text(f"🔄 Progresso: {progresso*100:.1f}%")
                
                if progresso < 1.0:
                    resultados_parciais.append(lote_processado)
                else:
                    result_df = lote_processado
            
            status_text.text("✅ Processamento concluído!")
            
        else:
            if total_linhas > 20000:
                st.warning("⏳ Processamento direto pode demorar para arquivos grandes...")
            
            # Atualizar progresso para processamento direto
            progress_bar.progress(0.3)
            status_text.text("🔧 Processamento direto em andamento...")
            
            result_df = processamento_direto_otimizado(data_df, config_df)
            
            progress_bar.progress(1.0)
            status_text.text("✅ Processamento concluído!")
        
        processing_time = time.time() - start_time
        
        # ==================================================
        # RESULTADOS E DOWNLOAD
        # ==================================================
        
        st.subheader("📊 Resultado Final")
        
        # Mostrar amostra inteligente
        linhas_para_mostrar = min(1000, len(result_df))
        if len(result_df) > 1000:
            st.info(f"📋 Mostrando {linhas_para_mostrar} de {len(result_df):,} linhas totais")
        
        st.dataframe(result_df.head(linhas_para_mostrar), use_container_width=True)
        
        # Estatísticas finais
        st.subheader("📈 Estatísticas do Processamento")
        
        stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
        
        with stat_col1:
            total_matches = 0
            for attr in config_df['Atributo'].unique():
                if attr in result_df.columns:
                    total_matches += (result_df[attr].str.count(',') + 1).where(result_df[attr] != '', 0).sum()
            st.metric("✅ Correspondências", f"{int(total_matches):,}")
        
        with stat_col2:
            atributos_com_match = sum([1 for attr in config_df['Atributo'].unique() 
                                     if attr in result_df.columns and result_df[attr].ne('').any()])
            st.metric("🎯 Atributos com Match", atributos_com_match)
        
        with stat_col3:
            if all(attr in result_df.columns for attr in config_df['Atributo'].unique()):
                linhas_com_match = result_df[config_df['Atributo'].unique()].ne('').any(axis=1).sum()
                st.metric("📝 Linhas com Match", f"{linhas_com_match:,}")
            else:
                st.metric("📝 Linhas com Match", "N/A")
        
        with stat_col4:
            st.metric("⏱️ Tempo Total", f"{processing_time:.1f}s")
            
            # Mostrar eficiência
            if processing_time > 0:
                eficiencia = total_linhas / processing_time
                st.caption(f"⚡ {eficiencia:.0f} linhas/segundo")
        
        # Download do resultado
        st.subheader("📥 Download do Resultado")
        
        if len(result_df) > 50000:
            st.warning("💡 Arquivo grande - Download em partes recomendado")
            partes = (len(result_df) // 50000) + 1
            for i in range(partes):
                inicio = i * 50000
                fim = min((i + 1) * 50000, len(result_df))
                parte_df = result_df.iloc[inicio:fim]
                
                with st.spinner(f"Preparando parte {i+1}..."):
                    parte_excel = to_excel(parte_df)
                
                st.download_button(
                    f"💾 Baixar Parte {i+1} (linhas {inicio+1}-{fim})", 
                    parte_excel, 
                    file_name=f"relatorio_parte_{i+1}.xlsx",
                    help=f"Parte {i+1} do relatório"
                )
        else:
            with st.spinner("Preparando arquivo para download..."):
                result_excel = to_excel(result_df)
            
            st.download_button(
                "💾 Baixar Relatório Completo", 
                result_excel, 
                file_name="relatorio_processado.xlsx",
                help="Planilha com os resultados do processamento",
                type="primary"
            )
        
        # Mensagem final personalizada
        if processing_time < 30:
            st.success(f"🎉 Processamento ultrarrápido concluído em {processing_time:.1f} segundos!")
        elif processing_time < 120:
            st.success(f"✅ Processamento eficiente concluído em {processing_time:.1f} segundos!")
        else:
            st.success(f"🐢 Processamento concluído em {processing_time:.1f} segundos. Para mais velocidade, considere o plano pago!")
        
        # Sugestão de upgrade se aplicável
        if is_render() and render_plan == "free" and total_linhas > 30000:
            st.info("""
            💡 **Dica de Performance:** Com plano pago este processamento seria **2-3x mais rápido** 
            e suportaria arquivos até **150.000+ linhas** com estabilidade!
            """)
        
    except Exception as e:
        st.error(f"❌ Erro durante o processamento: {str(e)}")
        
        # Sugestões específicas baseadas no erro
        if "memory" in str(e).lower():
            st.warning("""
            💡 **Problema de memória detectado:**
            - Divida o arquivo em partes menores
            - Use processamento em lotes com tamanho reduzido
            - Considere atualizar para plano pago com mais RAM
            """)
        elif "timeout" in str(e).lower():
            st.warning("""
            💡 **Timeout detectado:**
            - Reduza o tamanho do lote
            - Divida o arquivo manualmente
            - Plano pago oferece timeouts mais longos
            """)

else:
    st.info("👆 Faça o upload de ambas as planilhas para iniciar o processamento.")

# ==================================================
# SEÇÕES INFORMATIVAS PREMIUM
# ==================================================

with st.expander("🚀 Vantagens do Plano Pago"):
    st.markdown(f"""
    ### 📊 Comparação de Performance:
    
    | Recurso | Plano Free | **Plano Pago (US$7)** |
    |---------|------------|----------------------|
    | **Linhas máximas** | 50.000 | **{LIMITE_CRITICO_RENDER:,}** |
    | **Velocidade** | 1x | **2-3x mais rápido** |
    | **Estabilidade** | ⚠️ Limitada | **✅ Garantida** |
    | **Timeout** | 5 min | **10 min** |
    | **Suporte** | Básico | **Prioritário** |
    
    ### 💰 Custo-Benefício:
    - **US$7/mês** = menos de US$0.25 por dia
    - **Economia de tempo** significativa
    - **Processamento profissional** sem interrupções
    - **Suporte a clientes** com arquivos grandes
    
    [🔗 Atualizar para plano pago](https://render.com/pricing)
    """)

with st.expander("🔧 Otimizações Técnicas"):
    st.markdown(f"""
    ### ⚡ Configurações Ativas:
    - **Ambiente:** {'Render' if is_render() else 'Local'} 
    - **Plano:** {'Premium' if render_plan == 'paid' else 'Free'}
    - **Lote ótimo:** {TAMANHO_LOTE_OTIMO} linhas
    - **Timeout:** {TIMEOUT_PROCESSAMENTO//60} minutos
    - **Limite seguro:** {AVISO_LIMITE_RENDER:,} linhas
    
    ### 🛠️ Técnicas Aplicadas:
    - **Pré-compilação** de regex patterns
    - **Processamento em chunks** inteligentes
    - **Otimização de tipos** de dados
    - **Gerenciamento eficiente** de memória
    """)

with st.expander("📋 Exemplos de Uso Avançado"):
    st.markdown("""
    ### 🏭 Casos de Uso Empresariais:
    
    **🔧 Indústria de Componentes:**
    ```python
    Atributo: Voltagem → 110v, 220v, Bivolt
    Atributo: Material → Alumínio, Inox, Plástico
    Atributo: Aplicação → Industrial, Residencial
    ```
    
    **🛒 Varejo Eletrônico:**
    ```python
    Atributo: Tipo → LED, LCD, Plasma, OLED
    Atributo: Polegadas → 32, 40, 50, 55, 65
    Atributo: Smart → Sim, Não
    ```
    
    **🏗️ Material de Construção:**
    ```python
    Atributo: Cor → Branco, Preto, Cinza
    Atributo: Acabamento → Fosco, Brilhante
    Atributo: Tipo → Porcelanato, Cerâmica
    ```
    """)

# ==================================================
# RODAPÉ PREMIUM
# ==================================================

st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666;'>
    <p>🚀 <strong>Processador Premium</strong> • Otimizado para performance • Versão 3.0</p>
    <p><small>Desenvolvido para processamento profissional de grandes volumes de dados</small></p>
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
