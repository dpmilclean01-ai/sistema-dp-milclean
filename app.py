import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date, datetime, timedelta
import os
import time
import subprocess
import json
import numpy as np

# --- CONFIGURAÇÃO VISUAL ---
st.set_page_config(page_title="DP Milclean - V21", layout="wide")

st.markdown("""
<style>
    .stButton button { width: 100%; font-weight: bold; border-radius: 5px; }
    [data-testid="stMetricValue"] { font-size: 24px; font-weight: bold; }
    .stAlert { padding: 0.5rem; border-radius: 5px; margin-bottom: 10px; }
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# 0. CONSTANTES E UTILITÁRIOS
# ==============================================================================
COLUNAS_FIXAS = [
    'ID', 'FLUIG', 'MATRICULA', 'NOME', 'LOCACAO', 'DIAS_RECESSO', 'PERIODO_RECESSO', 
    'TIPO_DEMISSAO', 'DATA_DEMISSAO', 'TEM_CONSIGNADO', 'VALOR_CONSIGNADO', 
    'CALCULO_REALIZADO', 'DOC_ENVIADO', 'DATA_PAGAMENTO', 'FATURAMENTO', 
    'BAIXA_PAGAMENTO', 'OBSERVACOES', 'EXCLUIR'
]

SESSION_FILE = "user_session.json"

# --- TRADUTORES (O Segredo do Excel bonito) ---
def interpretar_booleano(valor):
    """Lê do Google (Texto) -> Vira Checkbox (True/False)"""
    v = str(valor).upper().strip()
    positivos = ['TRUE', '1', 'SIM', 'OK', 'CALCULADO', 'ENVIADO', 'PAGO', 'POSSUI FATURAMENTO', 'MARCADO']
    return True if any(x in v for x in positivos) else False

def formatar_para_texto(valor, tipo):
    """Lê do Checkbox (True/False) -> Vira Texto Bonito (Para Excel/Google)"""
    if tipo == 'CALCULO': return "CALCULADO" if valor else "PENDENTE"
    if tipo == 'DOC': return "ENVIADO" if valor else "PENDENTE"
    if tipo == 'PAGTO': return "PAGO" if valor else "ABERTO"
    if tipo == 'FAT': return "POSSUI FATURAMENTO" if valor else "NÃO"
    if tipo == 'EXCLUIR': return "MARCADO" if valor else ""
    return str(valor)

# ==============================================================================
# 1. LOGIN E CONEXÃO (AQUI ESTÁ A CORREÇÃO DA NUVEM)
# ==============================================================================
def save_session(user):
    with open(SESSION_FILE, "w") as f: json.dump({"user": user, "ts": time.time()}, f)

def load_session():
    if os.path.exists(SESSION_FILE):
        try:
            with open(SESSION_FILE, "r") as f:
                data = json.load(f)
                if time.time() - data.get("ts", 0) < 86400: return data.get("user")
        except: pass
    return None

def clear_session():
    if os.path.exists(SESSION_FILE): os.remove(SESSION_FILE)

if 'logado' not in st.session_state:
    saved = load_session()
    if saved:
        st.session_state['logado'] = True
        st.session_state['usuario_atual'] = saved
    else:
        st.session_state['logado'] = False
        st.session_state['usuario_atual'] = ''

@st.cache_resource
def conectar_gsheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # 1. TENTA LER DA NUVEM (SECRETS)
    if "gcp_service_account" in st.secrets:
        try:
            creds_dict = dict(st.secrets["gcp_service_account"])
            # Correção para chave privada com quebra de linha
            if "private_key" in creds_dict:
                creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
            
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
            client = gspread.authorize(creds)
            return client.open("SistemaDP_DB")
        except Exception as e:
            st.error(f"Erro nos Segredos da Nuvem: {e}")
            st.stop()

    # 2. TENTA LER DO ARQUIVO LOCAL (PC)
    elif os.path.exists("credenciais.json"):
        creds = ServiceAccountCredentials.from_json_keyfile_name("credenciais.json", scope)
        client = gspread.authorize(creds)
        return client.open("SistemaDP_DB")
        
    else:
        st.error("🚨 ERRO CRÍTICO: Não encontrei credenciais (Nem no arquivo local, nem nos Secrets da Nuvem).")
        st.stop()

def verificar_login(user, pwd):
    if user == "adm" and pwd == "123": return True
    try:
        sh = conectar_gsheets()
        ws = sh.worksheet("usuarios")
        df = pd.DataFrame(ws.get_all_records())
        df = df.astype(str)
        df.columns = [str(c).upper().strip() for c in df.columns]
        achou = df[df['USUARIO'] == str(user)]
        if not achou.empty:
            if str(pwd) == str(achou.iloc[0]['SENHA']): return True
    except: pass
    return False

if not st.session_state['logado']:
    st.markdown("## 🔒 DP Milclean")
    c1, c2 = st.columns([1, 2])
    with c1:
        u = st.text_input("Usuário")
        p = st.text_input("Senha", type="password")
        manter = st.checkbox("Mantenha-me conectado")
        if st.button("Entrar"):
            if verificar_login(u, p):
                st.session_state['logado'] = True
                st.session_state['usuario_atual'] = u
                if manter: save_session(u)
                st.rerun()
            else: st.error("Inválido")
    st.stop()

# ==============================================================================
# 2. CARREGAMENTO DE DADOS
# ==============================================================================
def limpar_matricula(valor):
    if pd.isna(valor) or str(valor).strip() == "": return ""
    return str(valor).strip().replace('.0', '')

@st.cache_data(ttl=60, show_spinner="Lendo bases...")
def carregar_bases():
    sh = conectar_gsheets()
    def ler(nome):
        try: return pd.DataFrame(sh.worksheet(nome).get_all_records())
        except: return pd.DataFrame()

    df_f = ler("base_funcionarios")
    if not df_f.empty:
        df_f.columns = [str(c).upper().strip() for c in df_f.columns]
        if 'MATRICULA' in df_f: df_f['MATRICULA'] = df_f['MATRICULA'].apply(limpar_matricula)

    df_c = ler("base_consignados")
    if not df_c.empty:
        df_c.columns = [str(c).upper().strip() for c in df_c.columns]
        if 'MATRICULA' in df_c: df_c['MATRICULA'] = df_c['MATRICULA'].apply(limpar_matricula)
        if 'VALOR' in df_c: df_c['VALOR'] = pd.to_numeric(df_c['VALOR'], errors='coerce').fillna(0)
        df_c = df_c.groupby('MATRICULA')['VALOR'].sum().reset_index()

    df_r = ler("base_recesso")
    if not df_r.empty:
        df_r.columns = [str(c).upper().strip() for c in df_r.columns]
        if 'MATRICULA' in df_r: df_r['MATRICULA'] = df_r['MATRICULA'].apply(limpar_matricula)
        if 'DIAS' in df_r:
            df_r['DIAS'] = df_r['DIAS'].astype(str).apply(lambda x: x.split(',')[0].split('.')[0])
            df_r['DIAS'] = pd.to_numeric(df_r['DIAS'], errors='coerce').fillna(0).astype(int)
        for col in ['PER_INI', 'PER_FIM']:
            if col in df_r: df_r[col] = pd.to_datetime(df_r[col], errors='coerce')
        df_r = df_r.drop_duplicates(subset=['MATRICULA'])

    return df_f, df_c, df_r

def buscar_dados(mat):
    df_f, df_c, df_r = carregar_bases()
    m = limpar_matricula(mat)
    nm, lc = "NOME MANUAL", "-"
    bf = df_f[df_f['MATRICULA'] == m]
    if not bf.empty:
        nm = bf.iloc[0].get('NOME', "Sem Nome")
        lc = bf.iloc[0].get('CENTRO_CUSTO', "-")
    vc = 0.0
    bc = df_c[df_c['MATRICULA'] == m]
    if not bc.empty: vc = float(bc.iloc[0]['VALOR'])
    dr, pr = 0, "-"
    br = df_r[df_r['MATRICULA'] == m]
    if not br.empty:
        dr = int(br.iloc[0]['DIAS'])
        di = br.iloc[0].get('PER_INI'); df = br.iloc[0].get('PER_FIM')
        if pd.notnull(di) and pd.notnull(df): pr = f"{di.strftime('%d/%m/%Y')} a {df.strftime('%d/%m/%Y')}"
    return nm, lc, vc, dr, pr

def registrar_log(acao, detalhes):
    try:
        sh = conectar_gsheets()
        ws = sh.worksheet("logs")
        ws.append_row([datetime.now().strftime('%d/%m/%Y %H:%M:%S'), st.session_state['usuario_atual'], acao, detalhes])
    except: pass

# ==============================================================================
# 3. INTERFACE
# ==============================================================================
with st.sidebar:
    st.write(f"👤 **{st.session_state['usuario_atual']}**")
    pagina = "Rescisões"
    if st.session_state['usuario_atual'] == 'adm': pagina = st.radio("Menu", ["Rescisões", "Gestão Usuários"])
    st.markdown("---")
    if st.button("🚀 ABRIR SISTEMA ANTIGO"):
        try: subprocess.Popen(r"C:\SistemaAntigo\Emissor.exe"); st.toast("Abrindo...")
        except: st.error("Erro exe")
    if st.button("🔄 FORÇAR RECARGA"):
        carregar_bases.clear(); st.cache_data.clear(); st.rerun()
    if st.button("Sair"):
        clear_session(); st.session_state['logado'] = False; st.rerun()

if pagina == "Rescisões":
    # --- CADASTRO ---
    with st.sidebar:
        st.header("➕ Novo")
        fluig = st.text_input("N° Fluig")
        mat = st.text_input("Matrícula").strip()
        nm, lc, vc, dr, pr = "", "", 0.0, 0, ""
        if mat:
            nm, lc, vc, dr, pr = buscar_dados(mat)
            if nm != "NOME MANUAL": st.success(f"✅ {nm}"); st.caption(f"📍 {lc}")
            else: st.warning("Nova Matrícula")
            if dr > 0: st.info(f"🏖️ Recesso: {dr} dias")
            if vc > 0: st.error(f"⚠️ Consignado: R$ {vc}")
        tipo = st.selectbox("Tipo", ["Aviso Trabalhado", "Aviso Indenizado", "Pedido de Demissão", "Término Contrato", "Acordo", "Rescisão Indireta"])
        dt_dem = st.date_input("Demissão", date.today(), format="DD/MM/YYYY")
        obs = st.text_area("Obs")
        
        if st.button("✅ SALVAR", type="primary"):
            if fluig and mat:
                try:
                    sh = conectar_gsheets()
                    ws = sh.worksheet("rescisões")
                    try: 
                        ids = ws.col_values(1)
                        nid = max([int(x) for x in ids[1:] if str(x).isdigit()]) + 1
                    except: nid = 1
                    
                    row = [
                        nid, f"'{fluig}", limpar_matricula(mat), nm, lc, dr, pr, tipo, 
                        dt_dem.strftime('%Y-%m-%d'), "Sim" if vc>0 else "Não", str(vc).replace('.',','),
                        "PENDENTE", "PENDENTE", (dt_dem+timedelta(10)).strftime('%Y-%m-%d'), "NÃO", "ABERTO", str(obs), ""
                    ]
                    
                    ws.append_row(row)
                    st.cache_data.clear(); st.success("SALVO!"); time.sleep(1); st.rerun()
                except Exception as e: st.error(f"Erro: {e}")
            else: st.error("Faltam dados")

    # --- TELA PRINCIPAL ---
    st.title("Gerenciamento de Rescisões")
    try:
        sh = conectar_gsheets()
        ws_res = sh.worksheet("rescisões")
        df = pd.DataFrame(ws_res.get_all_records())
    except: df = pd.DataFrame(columns=COLUNAS_FIXAS)

    if df.empty: df = pd.DataFrame(columns=COLUNAS_FIXAS)
    
    # Normalização
    df.columns = [str(c).upper().strip() for c in df.columns]
    for c in COLUNAS_FIXAS: 
        if c not in df.columns: df[c] = ""
    df = df[COLUNAS_FIXAS]

    # TRATAMENTO E TRADUÇÃO (LEITURA)
    if 'FLUIG' in df: df['FLUIG'] = df['FLUIG'].astype(str).str.replace("'", "")
    if 'MATRICULA' in df: df['MATRICULA'] = df['MATRICULA'].astype(str)
    for col in ['DATA_DEMISSAO', 'DATA_PAGAMENTO']:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
    
    bools = ['CALCULO_REALIZADO', 'DOC_ENVIADO', 'BAIXA_PAGAMENTO', 'FATURAMENTO', 'EXCLUIR']
    for b in bools:
        df[b] = df[b].apply(interpretar_booleano)

    # --- FILTROS ---
    st.markdown("#### 🔍 Filtros")
    c1, c2, c3, c4 = st.columns([1.5, 1.5, 1.5, 2])
    with c1: f_st = st.selectbox("Status", ["Todos", "Pendentes Cálculo", "Pendentes Doc", "Pendentes Pagto"])
    with c2: f_dt = st.selectbox("Data", ["Ignorar", "Demissão", "Pagamento"])
    with c3:
        h = date.today()
        di = st.date_input("De", h.replace(day=1), format="DD/MM/YYYY")
        dfim = st.date_input("Até", h, format="DD/MM/YYYY")
    with c4: busca = st.text_input("Buscar...")
    
    dfv = df.copy()
    if f_st == "Pendentes Cálculo": dfv = dfv[dfv['CALCULO_REALIZADO']==False]
    elif f_st == "Pendentes Doc": dfv = dfv[dfv['DOC_ENVIADO']==False]
    elif f_st == "Pendentes Pagto": dfv = dfv[dfv['BAIXA_PAGAMENTO']==False]
    if f_dt != "Ignorar":
        col = 'DATA_DEMISSAO' if f_dt == "Demissão" else 'DATA_PAGAMENTO'
        dfv = dfv[dfv[col].notna()]
        dfv = dfv[(dfv[col]>=di) & (dfv[col]<=dfim)]
    if busca: dfv = dfv[dfv.astype(str).apply(lambda x: x.str.contains(busca, case=False, na=False)).any(axis=1)]

    # --- ALERTAS (VOLTARAM!) ---
    p_calc = len(dfv[dfv['CALCULO_REALIZADO']==False])
    p_doc = len(dfv[dfv['DOC_ENVIADO']==False])
    p_pag = len(dfv[dfv['BAIXA_PAGAMENTO']==False])

    if p_calc > 0: st.error(f"🚨 Atenção: **{p_calc}** cálculos pendentes na visualização!")
    if p_doc > 0: st.warning(f"⚠️ Atenção: **{p_doc}** envios de documentos pendentes!")
    if p_pag > 0: st.info(f"💰 Atenção: **{p_pag}** pagamentos em aberto!")

    # DASHBOARD
    st.divider()
    st.caption(f"👁️ Visualizando: **{len(dfv)} registros**")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total", len(dfv))
    k2.metric("P. Cálculo", p_calc)
    k3.metric("P. Doc", p_doc)
    k4.metric("P. Pagto", p_pag)
    
    # EDITOR
    df_editado = st.data_editor(
        dfv,
        key="ed",
        num_rows="fixed",
        hide_index=True,
        use_container_width=True,
        column_config={
            "ID": st.column_config.NumberColumn(disabled=True, width="small"),
            "FLUIG": st.column_config.TextColumn("Fluig", width="small"),
            "MATRICULA": st.column_config.TextColumn("Matrícula", width="small"),
            "NOME": st.column_config.TextColumn(disabled=True),
            "LOCACAO": st.column_config.TextColumn(disabled=True),
            "DIAS_RECESSO": st.column_config.NumberColumn(disabled=True, width="small"),
            "PERIODO_RECESSO": st.column_config.TextColumn(disabled=True),
            "DATA_DEMISSAO": st.column_config.DateColumn(format="DD/MM/YYYY"),
            "DATA_PAGAMENTO": st.column_config.DateColumn(format="DD/MM/YYYY"),
            "CALCULO_REALIZADO": st.column_config.CheckboxColumn("Cálc?"),
            "DOC_ENVIADO": st.column_config.CheckboxColumn("Doc?"),
            "BAIXA_PAGAMENTO": st.column_config.CheckboxColumn("Pago?"),
            "FATURAMENTO": st.column_config.CheckboxColumn("Fat?"),
            "EXCLUIR": st.column_config.CheckboxColumn("Excluir?")
        }
    )
    
    c_save, c_del, c_exp = st.columns(3)

    # SALVAR
    with c_save:
        if 'confirm_save' not in st.session_state: st.session_state['confirm_save'] = False
        if st.button("💾 SINCRONIZAR TUDO", type="primary"): st.session_state['confirm_save'] = True
        
        if st.session_state['confirm_save']:
            st.warning("Confirma envio?")
            col_y, col_n = st.columns(2)
            if col_y.button("SIM"):
                try:
                    ws_res = sh.worksheet("rescisões")
                    df_g = pd.DataFrame(ws_res.get_all_records())
                    if df_g.empty: df_g = pd.DataFrame(columns=COLUNAS_FIXAS)
                    df_g.columns = [str(c).upper().strip() for c in df_g.columns]
                    
                    ids_t = df_editado['ID'].tolist()
                    df_keep = df_g[~df_g['ID'].isin(ids_t)]
                    df_new = df_editado.copy()
                    
                    # INTEGRIDADE
                    for i, r in df_new.iterrows():
                        nm, lc, vc, dr, pr = buscar_dados(str(r['MATRICULA']))
                        if r['NOME'] != nm:
                            df_new.at[i, 'NOME'] = nm; df_new.at[i, 'LOCACAO'] = lc
                            df_new.at[i, 'DIAS_RECESSO'] = dr; df_new.at[i, 'PERIODO_RECESSO'] = pr
                    
                    # FORMATAÇÃO (VOLTA PARA TEXTO)
                    if 'DATA_DEMISSAO' in df_new: df_new['DATA_DEMISSAO'] = df_new['DATA_DEMISSAO'].apply(lambda x: x.strftime('%Y-%m-%d') if x else "")
                    if 'DATA_PAGAMENTO' in df_new: df_new['DATA_PAGAMENTO'] = df_new['DATA_PAGAMENTO'].apply(lambda x: x.strftime('%Y-%m-%d') if x else "")
                    if 'FLUIG' in df_new: df_new['FLUIG'] = df_new['FLUIG'].astype(str).apply(lambda x: f"'{x}" if not str(x).startswith("'") else x)

                    # --- TRADUÇÃO (TEXTO PARA PLANILHA) ---
                    if 'CALCULO_REALIZADO' in df_new: df_new['CALCULO_REALIZADO'] = df_new['CALCULO_REALIZADO'].apply(lambda x: formatar_para_texto(x, 'CALCULO'))
                    if 'DOC_ENVIADO' in df_new: df_new['DOC_ENVIADO'] = df_new['DOC_ENVIADO'].apply(lambda x: formatar_para_texto(x, 'DOC'))
                    if 'BAIXA_PAGAMENTO' in df_new: df_new['BAIXA_PAGAMENTO'] = df_new['BAIXA_PAGAMENTO'].apply(lambda x: formatar_para_texto(x, 'PAGTO'))
                    if 'FATURAMENTO' in df_new: df_new['FATURAMENTO'] = df_new['FATURAMENTO'].apply(lambda x: formatar_para_texto(x, 'FAT'))
                    if 'EXCLUIR' in df_new: df_new['EXCLUIR'] = df_new['EXCLUIR'].apply(lambda x: formatar_para_texto(x, 'EXCLUIR'))

                    # Merge
                    for c in COLUNAS_FIXAS:
                        if c not in df_new.columns: df_new[c] = ""
                    df_fin = pd.concat([df_keep, df_new], ignore_index=True)
                    df_fin['ID'] = pd.to_numeric(df_fin['ID'], errors='coerce').fillna(0).astype(int)
                    df_fin = df_fin.sort_values('ID')
                    df_fin = df_fin[COLUNAS_FIXAS]
                    df_fin = df_fin.replace([np.inf, -np.inf, np.nan], "").fillna("")
                    
                    matriz = [df_fin.columns.values.tolist()] + df_fin.astype(str).values.tolist()
                    ws_res.clear()
                    ws_res.update(matriz)
                    
                    st.cache_data.clear()
                    st.session_state['confirm_save'] = False
                    st.success("Sincronizado!"); time.sleep(1); st.rerun()
                except Exception as e: st.error(f"Erro: {e}")
            if col_n.button("NÃO"): st.session_state['confirm_save'] = False; st.rerun()

    # DELETAR
    with c_del:
        to_del = df_editado[df_editado['EXCLUIR'] == True]
        if not to_del.empty:
            if 'confirm_del' not in st.session_state: st.session_state['confirm_del'] = False
            if st.button("🗑️ DELETAR"): st.session_state['confirm_del'] = True
            if st.session_state['confirm_del']:
                st.warning("Apagar?")
                dy, dn = st.columns(2)
                if dy.button("SIM"):
                    ws_res = sh.worksheet("rescisões")
                    df_g = pd.DataFrame(ws_res.get_all_records())
                    df_g.columns = [str(c).upper().strip() for c in df_g.columns]
                    ids = to_del['ID'].tolist()
                    fin = df_g[~df_g['ID'].isin(ids)]
                    for c in COLUNAS_FIXAS: 
                        if c not in fin.columns: fin[c] = ""
                    fin = fin[COLUNAS_FIXAS].replace([np.inf, -np.inf, np.nan], "").fillna("")
                    matriz = [fin.columns.values.tolist()] + fin.astype(str).values.tolist()
                    ws_res.clear(); ws_res.update(matriz)
                    st.cache_data.clear(); st.session_state['confirm_del'] = False
                    st.success("Feito!"); st.rerun()
                if dn.button("CANCELAR"): st.session_state['confirm_del'] = False; st.rerun()

    with c_exp:
        dx = df.copy() # Já está em booleanos, traduzimos agora pro excel
        if 'CALCULO_REALIZADO' in dx: dx['CALCULO_REALIZADO'] = dx['CALCULO_REALIZADO'].apply(lambda x: formatar_para_texto(x, 'CALCULO'))
        if 'DOC_ENVIADO' in dx: dx['DOC_ENVIADO'] = dx['DOC_ENVIADO'].apply(lambda x: formatar_para_texto(x, 'DOC'))
        if 'BAIXA_PAGAMENTO' in dx: dx['BAIXA_PAGAMENTO'] = dx['BAIXA_PAGAMENTO'].apply(lambda x: formatar_para_texto(x, 'PAGTO'))
        if 'FATURAMENTO' in dx: dx['FATURAMENTO'] = dx['FATURAMENTO'].apply(lambda x: formatar_para_texto(x, 'FAT'))
        if 'DATA_DEMISSAO' in dx: dx['DATA_DEMISSAO'] = pd.to_datetime(dx['DATA_DEMISSAO']).dt.strftime('%d/%m/%Y')
        csv = dx.to_csv(sep=';', decimal=',', index=False, encoding='utf-8-sig').encode('utf-8-sig')
        st.download_button("📥 Excel", csv, "res.csv")

elif pagina == "Gestão Usuários":
    st.title("Admin")
    c1, c2 = st.columns(2)
    sh = conectar_gsheets()
    ws_u = sh.worksheet("usuarios")
    with c1:
        st.subheader("Novo")
        with st.form("new"):
            nu = st.text_input("Login"); ns = st.text_input("Senha")
            if st.form_submit_button("Criar"): ws_u.append_row([nu, ns]); st.success("Criado!")
    with c2:
        st.subheader("Lista")
        d = ws_u.get_all_records()
        for x in d: st.write(f"👤 {x.get('USUARIO') or x.get('usuario')}")