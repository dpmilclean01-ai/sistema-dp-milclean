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
import io

# --- CONFIGURAÇÃO VISUAL ---
st.set_page_config(page_title="DP Milclean - V27", layout="wide")

st.markdown("""
<style>
    .stButton button { width: 100%; font-weight: bold; border-radius: 5px; }
    [data-testid="stMetricValue"] { font-size: 24px; font-weight: bold; }
    .stAlert { padding: 0.5rem; border-radius: 5px; margin-bottom: 10px; }
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# 0. CONSTANTES
# ==============================================================================
COLUNAS_FIXAS = [
    'ID', 'FLUIG', 'MATRICULA', 'NOME', 'CPF', 'PCD', 'LOCACAO',
    'DIAS_RECESSO', 'PERIODO_RECESSO', 'TIPO_DEMISSAO', 'DATA_DEMISSAO',
    'TEM_CONSIGNADO', 'VALOR_CONSIGNADO', 'CALCULO_REALIZADO', 'DOC_ENVIADO',
    'DATA_PAGAMENTO', 'FATURAMENTO', 'BAIXA_PAGAMENTO', 'OBSERVACOES',
    'SOLICITANTE',  # <-- NOVO (1)
    'EXCLUIR'
]

COLUNAS_BASE_FUNC = [
    "MATRICULA", "NOME", "CENTRO_CUSTO", "CPF", "PCD",
    "VIGENCIA_INICIO", "VIGENCIA_FIM", "ATIVO"
]

SESSION_FILE = "user_session.json"

# --- TRADUTORES ---
def interpretar_booleano(valor):
    v = str(valor).upper().strip()
    positivos = ['TRUE', '1', 'SIM', 'OK', 'CALCULADO', 'ENVIADO', 'PAGO', 'POSSUI FATURAMENTO', 'MARCADO']
    return True if any(x in v for x in positivos) else False

def formatar_para_texto(valor, tipo):
    if tipo == 'CALCULO': return "CALCULADO" if valor else "PENDENTE"
    if tipo == 'DOC': return "ENVIADO" if valor else "PENDENTE"
    if tipo == 'PAGTO': return "PAGO" if valor else "ABERTO"
    if tipo == 'FAT': return "POSSUI FATURAMENTO" if valor else "NÃO"
    if tipo == 'EXCLUIR': return "MARCADO" if valor else ""
    return str(valor)

# --- CORREÇÃO DEFINITIVA DA DATA (BR) ---
def formatar_data_para_salvar(valor):
    """Envia DD/MM/YYYY como TEXTO para o Google não bugar"""
    if pd.isna(valor) or valor == "" or valor is None: return ""
    if isinstance(valor, (date, datetime)): return valor.strftime('%d/%m/%Y')
    return str(valor)

def limpar_matricula(valor):
    if pd.isna(valor) or str(valor).strip() == "": return ""
    return str(valor).strip().replace('.0', '')

def to_excel_bytes(df: pd.DataFrame, sheet="Dados"):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet)
    return output.getvalue()

def norm_cols_upper(df):
    df.columns = [str(c).upper().strip() for c in df.columns]
    return df

def garantir_colunas_no_sheet(ws, colunas_necessarias):
    """
    Garante que o header da worksheet tenha as colunas necessárias.
    Se faltar, adiciona no final do header. Não apaga nada.
    """
    headers = [str(h).upper().strip() for h in ws.row_values(1)]
    faltantes = [c for c in colunas_necessarias if c not in headers]
    if not faltantes:
        return headers

    new_headers = headers + faltantes
    ws.update('1:1', [new_headers])

    # garante células vazias nas novas colunas (não obrigatório, mas evita confusão)
    # (não vamos preencher linha a linha para não estourar API)
    return new_headers

# ==============================================================================
# 1. LOGIN
# ==============================================================================
def save_session(user):
    with open(SESSION_FILE, "w") as f:
        json.dump({"user": user, "ts": time.time()}, f)

def load_session():
    if os.path.exists(SESSION_FILE):
        try:
            with open(SESSION_FILE, "r") as f:
                data = json.load(f)
                if time.time() - data.get("ts", 0) < 86400:
                    return data.get("user")
        except:
            pass
    return None

def clear_session():
    if os.path.exists(SESSION_FILE):
        os.remove(SESSION_FILE)

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
    if "gcp_service_account" in st.secrets:
        try:
            creds_dict = dict(st.secrets["gcp_service_account"])
            if "private_key" in creds_dict:
                creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
            client = gspread.authorize(creds)
            return client.open("SistemaDP_DB")
        except Exception as e:
            st.error(f"Erro Secrets: {e}")
            st.stop()
    elif os.path.exists("credenciais.json"):
        creds = ServiceAccountCredentials.from_json_keyfile_name("credenciais.json", scope)
        client = gspread.authorize(creds)
        return client.open("SistemaDP_DB")
    else:
        st.error("🚨 Credenciais não encontradas.")
        st.stop()

def verificar_login(user, pwd):
    if user == "adm" and pwd == "123":
        return True
    try:
        sh = conectar_gsheets()
        ws = sh.worksheet("usuarios")
        df = pd.DataFrame(ws.get_all_records())
        df = df.astype(str)
        df = norm_cols_upper(df)
        achou = df[df['USUARIO'] == str(user)]
        if not achou.empty:
            if str(pwd) == str(achou.iloc[0]['SENHA']):
                return True
    except:
        pass
    return False

def validar_sessao_ativa():
    if st.session_state['usuario_atual'] == 'adm':
        return True
    try:
        sh = conectar_gsheets()
        ws = sh.worksheet("usuarios")
        users = [str(u).upper() for u in ws.col_values(1)]
        if str(st.session_state['usuario_atual']).upper() not in users:
            return False
    except:
        return True
    return True

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
                if manter:
                    save_session(u)
                st.rerun()
            else:
                st.error("Inválido")
    st.stop()

if not validar_sessao_ativa():
    clear_session()
    st.session_state['logado'] = False
    st.error("🚫 Sessão encerrada.")
    time.sleep(2)
    st.rerun()

# ==============================================================================
# 2. CARREGAMENTO DE DADOS
# ==============================================================================
@st.cache_data(ttl=60, show_spinner="Lendo bases...")
def carregar_bases():
    sh = conectar_gsheets()

    def ler(nome):
        try:
            return pd.DataFrame(sh.worksheet(nome).get_all_records())
        except:
            return pd.DataFrame()

    df_f = ler("base_funcionarios")
    if not df_f.empty:
        df_f = norm_cols_upper(df_f)
        if 'MATRICULA' in df_f:
            df_f['MATRICULA'] = df_f['MATRICULA'].apply(limpar_matricula)
        if 'CPF' not in df_f:
            df_f['CPF'] = ""
        if 'PCD' not in df_f:
            df_f['PCD'] = "NÃO"
        # vigência (3)
        if 'VIGENCIA_INICIO' not in df_f:
            df_f['VIGENCIA_INICIO'] = ""
        if 'VIGENCIA_FIM' not in df_f:
            df_f['VIGENCIA_FIM'] = ""
        if 'ATIVO' not in df_f:
            df_f['ATIVO'] = "1"

    df_c = ler("base_consignados")
    if not df_c.empty:
        df_c = norm_cols_upper(df_c)
        if 'MATRICULA' in df_c:
            df_c['MATRICULA'] = df_c['MATRICULA'].apply(limpar_matricula)
        if 'VALOR' in df_c:
            df_c['VALOR'] = pd.to_numeric(df_c['VALOR'], errors='coerce').fillna(0)
        df_c = df_c.groupby('MATRICULA')['VALOR'].sum().reset_index()

    df_r = ler("base_recesso")
    if not df_r.empty:
        df_r = norm_cols_upper(df_r)
        if 'MATRICULA' in df_r:
            df_r['MATRICULA'] = df_r['MATRICULA'].apply(limpar_matricula)
        if 'DIAS' in df_r:
            df_r['DIAS'] = df_r['DIAS'].astype(str).apply(lambda x: x.split(',')[0].split('.')[0])
            df_r['DIAS'] = pd.to_numeric(df_r['DIAS'], errors='coerce').fillna(0).astype(int)
        for col in ['PER_INI', 'PER_FIM']:
            if col in df_r:
                df_r[col] = pd.to_datetime(df_r[col], errors='coerce')
        df_r = df_r.drop_duplicates(subset=['MATRICULA'])

    return df_f, df_c, df_r

def buscar_dados(mat, data_ref=None):
    """
    Busca funcionário por matrícula.
    Se existir controle de vigência, prefere ATIVO=1.
    """
    df_f, df_c, df_r = carregar_bases()
    m = limpar_matricula(mat)

    nm, lc, cpf, pcd = "NOME MANUAL", "-", "", "NÃO"

    if data_ref is None:
        data_ref = date.today()

    bf = df_f[df_f['MATRICULA'] == m] if (not df_f.empty and 'MATRICULA' in df_f) else pd.DataFrame()

    if not bf.empty:
        if 'ATIVO' in bf.columns:
            bf2 = bf[bf['ATIVO'].astype(str).str.upper().isin(['1','TRUE','SIM','ATIVO','OK'])]
            if not bf2.empty:
                bf = bf2
        row = bf.iloc[0]
        nm = row.get('NOME', "Sem Nome")
        lc = row.get('CENTRO_CUSTO', "-")
        cpf = row.get('CPF', "")
        pcd = row.get('PCD', "NÃO")

    vc = 0.0
    if not df_c.empty and 'MATRICULA' in df_c.columns:
        bc = df_c[df_c['MATRICULA'] == m]
        if not bc.empty:
            vc = float(bc.iloc[0]['VALOR'])

    dr, pr = 0, "-"
    if not df_r.empty and 'MATRICULA' in df_r.columns:
        br = df_r[df_r['MATRICULA'] == m]
        if not br.empty:
            dr = int(br.iloc[0].get('DIAS', 0))
            di = br.iloc[0].get('PER_INI')
            df_ = br.iloc[0].get('PER_FIM')
            if pd.notnull(di) and pd.notnull(df_):
                pr = f"{di.strftime('%d/%m/%Y')} a {df_.strftime('%d/%m/%Y')}"

    return nm, lc, cpf, pcd, vc, dr, pr

def listar_solicitantes():
    """(1) Dinâmico: busca na planilha rescisões valores já usados"""
    try:
        sh = conectar_gsheets()
        ws = sh.worksheet("rescisões")
        headers = [str(h).upper().strip() for h in ws.row_values(1)]
        if "SOLICITANTE" not in headers:
            return ["DP", "Gestor", "Financeiro", "Jurídico"]
        idx = headers.index("SOLICITANTE") + 1
        col = ws.col_values(idx)[1:]
        itens = sorted({str(x).strip() for x in col if str(x).strip()})
        base = ["DP", "Gestor", "Financeiro", "Jurídico"]
        return sorted(list(set(base + itens)))
    except:
        return ["DP", "Gestor", "Financeiro", "Jurídico"]

# ==============================================================================
# 3. INTERFACE
# ==============================================================================
with st.sidebar:
    st.write(f"👤 **{st.session_state['usuario_atual']}**")

    if st.session_state['usuario_atual'] == 'adm':
        pagina = st.radio("Menu", ["Rescisões", "Atualizar Bases", "Gestão Usuários"])
    else:
        pagina = st.radio("Menu", ["Rescisões", "Atualizar Bases"])

    st.markdown("---")
    if st.button("🚀 ABRIR SISTEMA ANTIGO"):
        try:
            subprocess.Popen(r"C:\SistemaAntigo\Emissor.exe")
            st.toast("Abrindo...")
        except:
            st.error("Erro exe local")
    if st.button("🔄 FORÇAR RECARGA"):
        carregar_bases.clear()
        st.cache_data.clear()
        st.rerun()
    if st.button("Sair"):
        clear_session()
        st.session_state['logado'] = False
        st.rerun()

# ==============================================================================
# 4. RESCISÕES
# ==============================================================================
if pagina == "Rescisões":
    # --- CADASTRO ---
    with st.sidebar:
        st.header("➕ Novo")
        fluig = st.text_input("N° Fluig")
        mat = st.text_input("Matrícula").strip()

        nm, lc, cpf, pcd, vc, dr, pr = "", "", "", "NÃO", 0.0, 0, ""
        if mat:
            nm, lc, cpf, pcd, vc, dr, pr = buscar_dados(mat)
            if nm != "NOME MANUAL":
                st.success(f"✅ {nm}")
                st.caption(f"📍 {lc}")
                st.caption(f"🆔 {cpf}")
                if str(pcd).upper() == "SIM":
                    st.error("♿ PCD: SIM")
                else:
                    st.info("PCD: NÃO")
            else:
                st.warning("Nova Matrícula")
            if dr > 0:
                st.warning(f"🏖️ Recesso: {dr} dias")
            if vc > 0:
                st.error(f"⚠️ Consignado: R$ {vc}")

        tipo = st.selectbox("Tipo", ["Aviso Trabalhado", "Aviso Indenizado", "Pedido de Demissão", "Término Contrato", "Acordo", "Rescisão Indireta"])
        dt_dem = st.date_input("Demissão", date.today(), format="DD/MM/YYYY")

        # (1) SOLICITANTE dinâmico + livre
        st.markdown("**Solicitante**")
        solicitantes = ["(digitar)"] + listar_solicitantes()
        sol_sel = st.selectbox("Lista", solicitantes, key="sol_sel")
        sol_txt = st.text_input("Ou digite", key="sol_txt")
        solicitante_final = sol_txt.strip() if sol_txt.strip() else ("" if sol_sel == "(digitar)" else sol_sel)

        obs = st.text_area("Obs")

        if st.button("✅ SALVAR", type="primary"):
            if fluig and mat:
                try:
                    sh = conectar_gsheets()
                    ws = sh.worksheet("rescisões")

                    # garante que a coluna SOLICITANTE exista no header (não quebra se você ainda não criou)
                    headers_planilha = garantir_colunas_no_sheet(ws, COLUNAS_FIXAS)

                    # ID
                    try:
                        col_id_idx = headers_planilha.index("ID") + 1
                        ids = ws.col_values(col_id_idx)
                        valid_ids = []
                        for x in ids[1:]:
                            if str(x).isdigit():
                                valid_ids.append(int(x))
                        nid = max(valid_ids) + 1 if valid_ids else 1
                    except:
                        nid = 1

                    dados_registro = {
                        'ID': nid,
                        'FLUIG': f"'{fluig}",
                        'MATRICULA': limpar_matricula(mat),
                        'NOME': nm,
                        'CPF': cpf,
                        'PCD': pcd,
                        'LOCACAO': lc,
                        'DIAS_RECESSO': dr,
                        'PERIODO_RECESSO': pr,
                        'TIPO_DEMISSAO': tipo,
                        'DATA_DEMISSAO': formatar_data_para_salvar(dt_dem),
                        'TEM_CONSIGNADO': "Sim" if vc > 0 else "Não",
                        'VALOR_CONSIGNADO': str(vc).replace('.', ','),
                        'CALCULO_REALIZADO': "PENDENTE",
                        'DOC_ENVIADO': "PENDENTE",
                        'DATA_PAGAMENTO': formatar_data_para_salvar(dt_dem + timedelta(days=10)),
                        'FATURAMENTO': "NÃO",
                        'BAIXA_PAGAMENTO': "ABERTO",
                        'OBSERVACOES': str(obs),
                        'SOLICITANTE': solicitante_final,  # <-- NOVO (1)
                        'EXCLUIR': ""
                    }

                    linha_final = []
                    for coluna in headers_planilha:
                        valor = dados_registro.get(coluna, "")
                        linha_final.append(valor)

                    ws.append_row(linha_final)

                    st.cache_data.clear()
                    st.success("SALVO!")
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro: {e}")
            else:
                st.error("Faltam dados")

    # --- TELA PRINCIPAL ---
    st.title("Gerenciamento de Rescisões")
    try:
        sh = conectar_gsheets()
        ws_res = sh.worksheet("rescisões")
        df = pd.DataFrame(ws_res.get_all_records())
    except:
        df = pd.DataFrame(columns=COLUNAS_FIXAS)

    if df.empty:
        df = pd.DataFrame(columns=COLUNAS_FIXAS)

    df = norm_cols_upper(df)

    # DATAS (LEITURA)
    for col in ['DATA_DEMISSAO', 'DATA_PAGAMENTO']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True).dt.date

    # GERAL
    if 'FLUIG' in df.columns:
        df['FLUIG'] = df['FLUIG'].astype(str).str.replace("'", "")
    if 'MATRICULA' in df.columns:
        df['MATRICULA'] = df['MATRICULA'].astype(str)

    bools = ['CALCULO_REALIZADO', 'DOC_ENVIADO', 'BAIXA_PAGAMENTO', 'FATURAMENTO', 'EXCLUIR']
    for b in bools:
        if b in df.columns:
            df[b] = df[b].apply(interpretar_booleano)

    # FILTROS
    st.markdown("#### 🔍 Filtros")
    c1, c2, c3, c4, c5 = st.columns([1.4, 1.4, 1.4, 2.2, 1.6])
    with c1:
        f_st = st.selectbox("Status", ["Todos", "Pendentes Cálculo", "Pendentes Doc", "Pendentes Pagto"])
    with c2:
        f_dt = st.selectbox("Data", ["Ignorar", "Demissão", "Pagamento"])
    with c3:
        h = date.today()
        di = st.date_input("De", h.replace(day=1), format="DD/MM/YYYY")
        dfim = st.date_input("Até", h, format="DD/MM/YYYY")
    with c4:
        busca = st.text_input("Buscar...")
    with c5:
        # (1) filtro por solicitante (dinâmico)
        if "SOLICITANTE" in df.columns:
            opts_sol = sorted({str(x).strip() for x in df["SOLICITANTE"].dropna().astype(str).tolist() if str(x).strip()})
            f_sol = st.multiselect("Solicitante", options=opts_sol)
        else:
            f_sol = []

    dfv = df.copy()
    if f_st == "Pendentes Cálculo" and 'CALCULO_REALIZADO' in dfv.columns:
        dfv = dfv[dfv['CALCULO_REALIZADO'] == False]
    elif f_st == "Pendentes Doc" and 'DOC_ENVIADO' in dfv.columns:
        dfv = dfv[dfv['DOC_ENVIADO'] == False]
    elif f_st == "Pendentes Pagto" and 'BAIXA_PAGAMENTO' in dfv.columns:
        dfv = dfv[dfv['BAIXA_PAGAMENTO'] == False]

    if f_dt != "Ignorar":
        col = 'DATA_DEMISSAO' if f_dt == "Demissão" else 'DATA_PAGAMENTO'
        if col in dfv.columns:
            dfv = dfv[dfv[col].notna()]
            dfv = dfv[(dfv[col] >= di) & (dfv[col] <= dfim)]

    if busca:
        dfv = dfv[dfv.astype(str).apply(lambda x: x.str.contains(busca, case=False, na=False)).any(axis=1)]

    if f_sol and "SOLICITANTE" in dfv.columns:
        dfv = dfv[dfv["SOLICITANTE"].astype(str).isin(f_sol)]

    # DASHBOARD ALERTAS
    p_calc = len(dfv[dfv['CALCULO_REALIZADO'] == False]) if 'CALCULO_REALIZADO' in dfv.columns else 0
    p_doc = len(dfv[dfv['DOC_ENVIADO'] == False]) if 'DOC_ENVIADO' in dfv.columns else 0
    p_pag = len(dfv[dfv['BAIXA_PAGAMENTO'] == False]) if 'BAIXA_PAGAMENTO' in dfv.columns else 0

    if p_calc > 0: st.error(f"🚨 **{p_calc}** cálculos pendentes!")
    if p_doc > 0: st.warning(f"⚠️ **{p_doc}** envios pendentes!")
    if p_pag > 0: st.info(f"💰 **{p_pag}** pagamentos abertos!")

    st.divider()
    st.caption(f"👁️ Visualizando: **{len(dfv)} registros**")

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
            "CPF": st.column_config.TextColumn(disabled=True),
            "PCD": st.column_config.TextColumn(disabled=True, width="small"),
            "LOCACAO": st.column_config.TextColumn(disabled=True),
            "DIAS_RECESSO": st.column_config.NumberColumn(disabled=True, width="small"),
            "PERIODO_RECESSO": st.column_config.TextColumn(disabled=True),
            "DATA_DEMISSAO": st.column_config.DateColumn(format="DD/MM/YYYY"),
            "DATA_PAGAMENTO": st.column_config.DateColumn(format="DD/MM/YYYY"),
            "CALCULO_REALIZADO": st.column_config.CheckboxColumn("Cálc?"),
            "DOC_ENVIADO": st.column_config.CheckboxColumn("Doc?"),
            "BAIXA_PAGAMENTO": st.column_config.CheckboxColumn("Pago?"),
            "FATURAMENTO": st.column_config.CheckboxColumn("Fat?"),
            "SOLICITANTE": st.column_config.TextColumn("Solicitante"),
            "EXCLUIR": st.column_config.CheckboxColumn("Excluir?")
        }
    )

    c_save, c_del, c_exp = st.columns(3)

    # SALVAR (sincroniza tudo que está na tela filtrada)
    with c_save:
        if 'confirm_save' not in st.session_state:
            st.session_state['confirm_save'] = False
        if st.button("💾 SINCRONIZAR TUDO", type="primary"):
            st.session_state['confirm_save'] = True

        if st.session_state['confirm_save']:
            st.warning("Confirma envio?")
            col_y, col_n = st.columns(2)
            if col_y.button("SIM"):
                try:
                    ws_res = sh.worksheet("rescisões")
                    headers_planilha = garantir_colunas_no_sheet(ws_res, COLUNAS_FIXAS)

                    df_g = pd.DataFrame(ws_res.get_all_records())
                    if df_g.empty:
                        df_g = pd.DataFrame(columns=headers_planilha)

                    df_g = norm_cols_upper(df_g)

                    ids_t = df_editado['ID'].tolist()
                    if 'ID' in df_g.columns:
                        df_keep = df_g[~df_g['ID'].isin(ids_t)]
                    else:
                        df_keep = df_g.copy()

                    df_new = df_editado.copy()

                    # INTEGRIDADE
                    for i, r in df_new.iterrows():
                        nm2, lc2, cpf2, pcd2, vc2, dr2, pr2 = buscar_dados(str(r['MATRICULA']))
                        if str(r.get('NOME', "")) != str(nm2):
                            df_new.at[i, 'NOME'] = nm2
                            df_new.at[i, 'LOCACAO'] = lc2
                            df_new.at[i, 'CPF'] = cpf2
                            df_new.at[i, 'PCD'] = pcd2
                            df_new.at[i, 'DIAS_RECESSO'] = dr2
                            df_new.at[i, 'PERIODO_RECESSO'] = pr2
                        # consignado (mantém coerente)
                        df_new.at[i, 'TEM_CONSIGNADO'] = "Sim" if float(vc2) > 0 else "Não"
                        df_new.at[i, 'VALOR_CONSIGNADO'] = str(vc2).replace(".", ",")

                    # FORMATAÇÃO
                    if 'DATA_DEMISSAO' in df_new.columns:
                        df_new['DATA_DEMISSAO'] = df_new['DATA_DEMISSAO'].apply(formatar_data_para_salvar)
                    if 'DATA_PAGAMENTO' in df_new.columns:
                        df_new['DATA_PAGAMENTO'] = df_new['DATA_PAGAMENTO'].apply(formatar_data_para_salvar)
                    if 'FLUIG' in df_new.columns:
                        df_new['FLUIG'] = df_new['FLUIG'].astype(str).apply(lambda x: f"'{x}" if not str(x).startswith("'") else x)

                    # TRADUÇÃO CHECKBOX
                    if 'CALCULO_REALIZADO' in df_new.columns:
                        df_new['CALCULO_REALIZADO'] = df_new['CALCULO_REALIZADO'].apply(lambda x: formatar_para_texto(x, 'CALCULO'))
                    if 'DOC_ENVIADO' in df_new.columns:
                        df_new['DOC_ENVIADO'] = df_new['DOC_ENVIADO'].apply(lambda x: formatar_para_texto(x, 'DOC'))
                    if 'BAIXA_PAGAMENTO' in df_new.columns:
                        df_new['BAIXA_PAGAMENTO'] = df_new['BAIXA_PAGAMENTO'].apply(lambda x: formatar_para_texto(x, 'PAGTO'))
                    if 'FATURAMENTO' in df_new.columns:
                        df_new['FATURAMENTO'] = df_new['FATURAMENTO'].apply(lambda x: formatar_para_texto(x, 'FAT'))
                    if 'EXCLUIR' in df_new.columns:
                        df_new['EXCLUIR'] = df_new['EXCLUIR'].apply(lambda x: formatar_para_texto(x, 'EXCLUIR'))

                    # MERGE
                    df_fin = pd.concat([df_keep, df_new], ignore_index=True)
                    if 'ID' in df_fin.columns:
                        df_fin['ID'] = pd.to_numeric(df_fin['ID'], errors='coerce').fillna(0).astype(int)
                        df_fin = df_fin.sort_values('ID')

                    # Ordena colunas pelo header real
                    for c in headers_planilha:
                        if c not in df_fin.columns:
                            df_fin[c] = ""
                    df_fin = df_fin[headers_planilha]

                    df_fin = df_fin.replace([np.inf, -np.inf, np.nan], "").fillna("")
                    matriz = [df_fin.columns.values.tolist()] + df_fin.astype(str).values.tolist()
                    ws_res.clear()
                    ws_res.update(matriz)

                    st.cache_data.clear()
                    st.session_state['confirm_save'] = False
                    st.success("Sincronizado!")
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro: {e}")
            if col_n.button("NÃO"):
                st.session_state['confirm_save'] = False
                st.rerun()

    # DELETAR (por EXCLUIR)
    with c_del:
        to_del = df_editado[df_editado['EXCLUIR'] == True] if 'EXCLUIR' in df_editado.columns else pd.DataFrame()
        if not to_del.empty:
            if 'confirm_del' not in st.session_state:
                st.session_state['confirm_del'] = False
            if st.button("🗑️ DELETAR"):
                st.session_state['confirm_del'] = True
            if st.session_state['confirm_del']:
                st.warning("Apagar?")
                dy, dn = st.columns(2)
                if dy.button("SIM"):
                    ws_res = sh.worksheet("rescisões")
                    headers_planilha = garantir_colunas_no_sheet(ws_res, COLUNAS_FIXAS)

                    df_g = pd.DataFrame(ws_res.get_all_records())
                    df_g = norm_cols_upper(df_g) if not df_g.empty else pd.DataFrame(columns=headers_planilha)

                    ids = to_del['ID'].tolist() if 'ID' in to_del.columns else []
                    if 'ID' in df_g.columns:
                        fin = df_g[~df_g['ID'].isin(ids)]
                    else:
                        fin = df_g.copy()

                    for c in headers_planilha:
                        if c not in fin.columns:
                            fin[c] = ""
                    fin = fin[headers_planilha].replace([np.inf, -np.inf, np.nan], "").fillna("")

                    matriz = [fin.columns.values.tolist()] + fin.astype(str).values.tolist()
                    ws_res.clear()
                    ws_res.update(matriz)

                    st.cache_data.clear()
                    st.session_state['confirm_del'] = False
                    st.success("Feito!")
                    st.rerun()
                if dn.button("CANCELAR"):
                    st.session_state['confirm_del'] = False
                    st.rerun()

    # EXPORTAR (2) AGORA EXPORTA SOMENTE O FILTRADO (dfv)
    with c_exp:
        st.markdown("**Exportar (somente filtrados)**")

        dx = dfv.copy()

        if 'CALCULO_REALIZADO' in dx.columns:
            dx['CALCULO_REALIZADO'] = dx['CALCULO_REALIZADO'].apply(lambda x: formatar_para_texto(x, 'CALCULO'))
        if 'DOC_ENVIADO' in dx.columns:
            dx['DOC_ENVIADO'] = dx['DOC_ENVIADO'].apply(lambda x: formatar_para_texto(x, 'DOC'))
        if 'BAIXA_PAGAMENTO' in dx.columns:
            dx['BAIXA_PAGAMENTO'] = dx['BAIXA_PAGAMENTO'].apply(lambda x: formatar_para_texto(x, 'PAGTO'))
        if 'FATURAMENTO' in dx.columns:
            dx['FATURAMENTO'] = dx['FATURAMENTO'].apply(lambda x: formatar_para_texto(x, 'FAT'))

        if 'DATA_DEMISSAO' in dx.columns:
            dx['DATA_DEMISSAO'] = pd.to_datetime(dx['DATA_DEMISSAO'], errors='coerce').dt.strftime('%d/%m/%Y')
        if 'DATA_PAGAMENTO' in dx.columns:
            dx['DATA_PAGAMENTO'] = pd.to_datetime(dx['DATA_PAGAMENTO'], errors='coerce').dt.strftime('%d/%m/%Y')

        # CSV (rápido)
        csv = dx.to_csv(sep=';', decimal=',', index=False, encoding='utf-8-sig').encode('utf-8-sig')
        st.download_button("📥 CSV (filtrado)", csv, "res_filtrado.csv")

        # XLSX real
        try:
            xlsx = to_excel_bytes(dx, "RescisoesFiltrado")
            st.download_button("📥 Excel (filtrado)", xlsx, "res_filtrado.xlsx")
        except Exception as e:
            st.error(f"Falha ao gerar Excel: {e}")

# ==============================================================================
# 5. ATUALIZAR BASES (3)
# ==============================================================================
elif pagina == "Atualizar Bases":
    st.title("Atualização de Bases (sem apagar)")

    st.info(
        "Aqui você atualiza a base de funcionários sem apagar o que já existe.\n\n"
        "✅ Se matrícula já existe ativa: o sistema encerra a vigência anterior (VIGENCIA_FIM) e cria uma nova linha ativa.\n"
        "✅ Se matrícula é nova: apenas adiciona.\n"
    )

    sh = conectar_gsheets()

    st.subheader("Base de Funcionários")
    colA, colB = st.columns([1.2, 2])

    with colA:
        vig_ini = st.date_input("Data de início da vigência", value=date.today(), format="DD/MM/YYYY")
        arquivo = st.file_uploader("Upload CSV (;) ou XLSX", type=["csv", "xlsx"])
        st.caption("Colunas mínimas no arquivo: MATRICULA, NOME, CENTRO_CUSTO (opcional), CPF (opcional), PCD (opcional)")

    with colB:
        try:
            ws = sh.worksheet("base_funcionarios")
            headers = garantir_colunas_no_sheet(ws, COLUNAS_BASE_FUNC)

            df_atual = pd.DataFrame(ws.get_all_records())
            df_atual = norm_cols_upper(df_atual) if not df_atual.empty else pd.DataFrame(columns=headers)

            # garante colunas
            for c in COLUNAS_BASE_FUNC:
                if c not in df_atual.columns:
                    df_atual[c] = ""

            # normaliza
            df_atual["MATRICULA"] = df_atual["MATRICULA"].apply(limpar_matricula)
            st.caption(f"Registros atuais na base: {len(df_atual)}")
            st.download_button("📥 Baixar base atual (Excel)", to_excel_bytes(df_atual, "BaseAtual"), "base_funcionarios_atual.xlsx")
            st.dataframe(df_atual.tail(50), use_container_width=True)
        except Exception as e:
            st.error(f"Erro ao ler base_funcionarios: {e}")
            st.stop()

    st.divider()

    if arquivo and st.button("✅ Atualizar base agora", type="primary"):
        try:
            # lê arquivo
            if arquivo.name.lower().endswith(".xlsx"):
                df_new = pd.read_excel(arquivo, dtype=str)
            else:
                df_new = pd.read_csv(arquivo, sep=";", dtype=str)

            df_new = norm_cols_upper(df_new)

            # valida
            if "MATRICULA" not in df_new.columns or "NOME" not in df_new.columns:
                st.error("Seu arquivo precisa ter pelo menos as colunas: MATRICULA e NOME.")
                st.stop()

            # completa colunas opcionais
            for c in ["CENTRO_CUSTO", "CPF", "PCD"]:
                if c not in df_new.columns:
                    df_new[c] = ""

            df_new["MATRICULA"] = df_new["MATRICULA"].apply(limpar_matricula)
            df_new = df_new[df_new["MATRICULA"].astype(str).str.strip() != ""].copy()

            # garante colunas de vigência no novo
            df_new["VIGENCIA_INICIO"] = formatar_data_para_salvar(vig_ini)
            df_new["VIGENCIA_FIM"] = ""
            df_new["ATIVO"] = "1"

            # base atual (recarrega, para garantir consistência)
            ws = sh.worksheet("base_funcionarios")
            headers = garantir_colunas_no_sheet(ws, COLUNAS_BASE_FUNC)

            df_atual = pd.DataFrame(ws.get_all_records())
            df_atual = norm_cols_upper(df_atual) if not df_atual.empty else pd.DataFrame(columns=headers)

            for c in COLUNAS_BASE_FUNC:
                if c not in df_atual.columns:
                    df_atual[c] = ""

            df_atual["MATRICULA"] = df_atual["MATRICULA"].apply(limpar_matricula)
            df_atual["ATIVO"] = df_atual["ATIVO"].astype(str)

            mats_upd = set(df_new["MATRICULA"].tolist())
            dia_anterior = formatar_data_para_salvar(vig_ini - timedelta(days=1))

            # encerra vigência atual
            mask_ativos = (df_atual["MATRICULA"].isin(mats_upd)) & (df_atual["ATIVO"].str.upper().isin(["1", "TRUE", "SIM", "ATIVO", "OK"]))
            df_atual.loc[mask_ativos, "VIGENCIA_FIM"] = dia_anterior
            df_atual.loc[mask_ativos, "ATIVO"] = "0"

            # adiciona novas linhas
            cols_final = COLUNAS_BASE_FUNC[:]  # ordem
            df_add = df_new[cols_final].copy()

            # mantém outras colunas extras existentes
            extras = [c for c in df_atual.columns if c not in cols_final]
            for c in extras:
                if c not in df_add.columns:
                    df_add[c] = ""

            df_final = pd.concat([df_atual, df_add], ignore_index=True)

            # ordena
            if "MATRICULA" in df_final.columns and "VIGENCIA_INICIO" in df_final.columns:
                df_final = df_final.sort_values(by=["MATRICULA", "VIGENCIA_INICIO"], kind="stable")

            # garante header final com tudo que existe
            headers_final = sorted(list(set(headers + df_final.columns.tolist())), key=lambda x: headers.index(x) if x in headers else 9999)
            for c in headers_final:
                if c not in df_final.columns:
                    df_final[c] = ""

            df_final = df_final[headers_final].replace([np.inf, -np.inf, np.nan], "").fillna("")
            matriz = [df_final.columns.values.tolist()] + df_final.astype(str).values.tolist()

            ws.clear()
            ws.update(matriz)

            st.cache_data.clear()
            carregar_bases.clear()

            st.success(f"✅ Base atualizada. Matrículas processadas: {len(mats_upd)}")
            st.download_button("📥 Baixar base atualizada (Excel)", to_excel_bytes(df_final, "BaseAtualizada"), "base_funcionarios_atualizada.xlsx")
            st.rerun()

        except Exception as e:
            st.error(f"Erro na atualização: {e}")

# ==============================================================================
# 6. GESTÃO USUÁRIOS
# ==============================================================================
elif pagina == "Gestão Usuários":
    st.title("Admin")
    c1, c2 = st.columns(2)
    sh = conectar_gsheets()
    ws_u = sh.worksheet("usuarios")
    with c1:
        st.subheader("Novo")
        with st.form("new"):
            nu = st.text_input("Login")
            ns = st.text_input("Senha")
            if st.form_submit_button("Criar"):
                ws_u.append_row([nu, ns])
                st.success("Criado!")
    with c2:
        st.subheader("Ativos")
        d = ws_u.get_all_records()
        if d:
            df_u = pd.DataFrame(d)
            st.dataframe(df_u, use_container_width=True)
            u_del = st.selectbox("Derrubar:", df_u['USUARIO'].tolist())
            if st.button(f"🚫 EXCLUIR {u_del}"):
                novos = df_u[df_u['USUARIO'] != u_del]
                ws_u.clear()
                ws_u.update([novos.columns.values.tolist()] + novos.values.tolist())
                st.success("Feito!")
                time.sleep(1)
                st.rerun()
