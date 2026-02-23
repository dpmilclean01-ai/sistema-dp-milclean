import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date, datetime, timedelta
import time
import json
import numpy as np
import io

# ==============================================================================
# CONFIG VISUAL
# ==============================================================================
st.set_page_config(page_title="DP Milclean - V28 (Cloud)", layout="wide")

st.markdown("""
<style>
    .stButton button { width: 100%; font-weight: bold; border-radius: 6px; }
    [data-testid="stMetricValue"] { font-size: 24px; font-weight: bold; }
    .stAlert { padding: 0.5rem; border-radius: 6px; margin-bottom: 10px; }
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# CONSTANTES
# ==============================================================================
SESSION_FILE = "user_session.json"  # no cloud pode não persistir sempre; ok

COLUNAS_RESCISOES_MIN = [
    'ID', 'FLUIG', 'MATRICULA', 'NOME', 'CPF', 'PCD', 'LOCACAO',
    'DIAS_RECESSO', 'PERIODO_RECESSO', 'TIPO_DEMISSAO', 'DATA_DEMISSAO',
    'TEM_CONSIGNADO', 'VALOR_CONSIGNADO', 'CALCULO_REALIZADO', 'DOC_ENVIADO',
    'DATA_PAGAMENTO', 'FATURAMENTO', 'BAIXA_PAGAMENTO', 'OBSERVACOES',
    'SOLICITANTE', 'EXCLUIR'
]

# ==============================================================================
# HELPERS
# ==============================================================================
def norm_cols_upper(df):
    df.columns = [str(c).upper().strip() for c in df.columns]
    return df

def limpar_matricula(valor):
    if pd.isna(valor) or str(valor).strip() == "":
        return ""
    return str(valor).strip().replace(".0", "")

def interpretar_booleano(valor):
    v = str(valor).upper().strip()
    positivos = ['TRUE', '1', 'SIM', 'OK', 'CALCULADO', 'ENVIADO', 'PAGO', 'POSSUI FATURAMENTO', 'MARCADO', 'ABERTO']
    return True if any(x in v for x in positivos) else False

def formatar_para_texto(valor, tipo):
    if tipo == 'CALCULO': return "CALCULADO" if valor else "PENDENTE"
    if tipo == 'DOC': return "ENVIADO" if valor else "PENDENTE"
    if tipo == 'PAGTO': return "PAGO" if valor else "ABERTO"
    if tipo == 'FAT': return "POSSUI FATURAMENTO" if valor else "NÃO"
    if tipo == 'EXCLUIR': return "MARCADO" if valor else ""
    return str(valor)

def formatar_data_para_salvar(valor):
    if pd.isna(valor) or valor == "" or valor is None:
        return ""
    if isinstance(valor, (date, datetime)):
        return valor.strftime('%d/%m/%Y')
    return str(valor)

def to_excel_bytes(df: pd.DataFrame, sheet="Dados"):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet)
    return output.getvalue()

# ==============================================================================
# CONEXÃO GOOGLE SHEETS (SOMENTE CLOUD)
# ==============================================================================
@st.cache_resource
def conectar_gsheets():
    # Não deixa o Streamlit mostrar "No secrets found" no meio da tela
    if "gcp_service_account" not in st.secrets:
        st.error("❌ Secrets não configurado no Streamlit Cloud. Configure o bloco [gcp_service_account] no Settings > Secrets.")
        st.stop()

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = dict(st.secrets["gcp_service_account"])
    if "private_key" in creds_dict:
        creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    # Nome do arquivo no Google Drive
    return client.open("SistemaDP_DB")

def garantir_colunas_no_sheet(ws, colunas_necessarias):
    headers = [str(h).upper().strip() for h in ws.row_values(1)]
    faltantes = [c for c in colunas_necessarias if c not in headers]
    if not faltantes:
        return headers
    novo = headers + faltantes
    ws.update('1:1', [novo])
    return novo

# ==============================================================================
# LOGIN
# ==============================================================================
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
            return str(pwd) == str(achou.iloc[0].get('SENHA', ''))
    except:
        pass
    return False

if "logado" not in st.session_state:
    st.session_state["logado"] = False
    st.session_state["usuario_atual"] = ""

if not st.session_state["logado"]:
    st.markdown("## 🔒 DP Milclean")
    u = st.text_input("Usuário")
    p = st.text_input("Senha", type="password")
    if st.button("Entrar"):
        if verificar_login(u, p):
            st.session_state["logado"] = True
            st.session_state["usuario_atual"] = u
            st.rerun()
        else:
            st.error("Usuário/Senha inválidos")
    st.stop()

# ==============================================================================
# CARREGAMENTO BASES
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
        if 'MATRICULA' in df_f.columns:
            df_f['MATRICULA'] = df_f['MATRICULA'].apply(limpar_matricula)
        if 'CPF' not in df_f.columns: df_f['CPF'] = ""
        if 'PCD' not in df_f.columns: df_f['PCD'] = "NÃO"
        # opcional: coluna para controle de atualização
        if 'ATUALIZADO_EM' not in df_f.columns: df_f['ATUALIZADO_EM'] = ""

    df_c = ler("base_consignados")
    if not df_c.empty:
        df_c = norm_cols_upper(df_c)
        if 'MATRICULA' in df_c.columns:
            df_c['MATRICULA'] = df_c['MATRICULA'].apply(limpar_matricula)
        if 'VALOR' in df_c.columns:
            df_c['VALOR'] = pd.to_numeric(df_c['VALOR'], errors='coerce').fillna(0)
        df_c = df_c.groupby('MATRICULA')['VALOR'].sum().reset_index()

    df_r = ler("base_recesso")
    if not df_r.empty:
        df_r = norm_cols_upper(df_r)
        if 'MATRICULA' in df_r.columns:
            df_r['MATRICULA'] = df_r['MATRICULA'].apply(limpar_matricula)
        if 'DIAS' in df_r.columns:
            df_r['DIAS'] = df_r['DIAS'].astype(str).apply(lambda x: x.split(',')[0].split('.')[0])
            df_r['DIAS'] = pd.to_numeric(df_r['DIAS'], errors='coerce').fillna(0).astype(int)
        for col in ['PER_INI', 'PER_FIM']:
            if col in df_r.columns:
                df_r[col] = pd.to_datetime(df_r[col], errors='coerce')
        df_r = df_r.drop_duplicates(subset=['MATRICULA'])

    return df_f, df_c, df_r

def buscar_dados(mat):
    df_f, df_c, df_r = carregar_bases()
    m = limpar_matricula(mat)

    nm, lc, cpf, pcd = "NOME MANUAL", "-", "", "NÃO"
    if not df_f.empty and 'MATRICULA' in df_f.columns:
        bf = df_f[df_f['MATRICULA'] == m]
        if not bf.empty:
            row = bf.iloc[0]
            nm = row.get('NOME', "Sem Nome")
            # seu campo atual é CENTRO_CUSTO, mas tela chama LOCACAO
            lc = row.get('CENTRO_CUSTO', row.get('LOCACAO', "-"))
            cpf = row.get('CPF', "")
            pcd = row.get('PCD', "NÃO")

    vc = 0.0
    if not df_c.empty and 'MATRICULA' in df_c.columns:
        bc = df_c[df_c['MATRICULA'] == m]
        if not bc.empty:
            vc = float(bc.iloc[0].get('VALOR', 0))

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

def listar_solicitantes_existentes():
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
# SIDEBAR
# ==============================================================================
with st.sidebar:
    st.write(f"👤 **{st.session_state['usuario_atual']}**")
    pagina = st.radio("Menu", ["Rescisões", "Atualizar Base"] + (["Gestão Usuários"] if st.session_state['usuario_atual'] == 'adm' else []))
    st.markdown("---")
    if st.button("🔄 FORÇAR RECARGA"):
        carregar_bases.clear()
        st.cache_data.clear()
        st.rerun()
    if st.button("Sair"):
        st.session_state["logado"] = False
        st.session_state["usuario_atual"] = ""
        st.rerun()

# ==============================================================================
# PÁGINA: RESCISÕES
# ==============================================================================
if pagina == "Rescisões":
    # ---------- CADASTRO ----------
    with st.sidebar:
        st.header("➕ Novo Registro")

        fluig = st.text_input("N° Fluig")
        mat = st.text_input("Matrícula").strip()

        nm, lc, cpf, pcd, vc, dr, pr = "", "", "", "NÃO", 0.0, 0, ""
        if mat:
            nm, lc, cpf, pcd, vc, dr, pr = buscar_dados(mat)
            if nm != "NOME MANUAL":
                st.success(f"✅ {nm}")
                st.caption(f"📍 {lc}")
                st.caption(f"🆔 {cpf}")
                st.caption(f"PCD: {pcd}")
            else:
                st.warning("⚠️ Matrícula não encontrada (nome manual).")

            if dr > 0: st.warning(f"🏖️ Recesso: {dr} dias")
            if vc > 0: st.error(f"⚠️ Consignado: R$ {vc:.2f}")

        tipo = st.selectbox("Tipo", ["Aviso Trabalhado", "Aviso Indenizado", "Pedido de Demissão", "Término Contrato", "Acordo", "Rescisão Indireta"])
        dt_dem = st.date_input("Demissão", date.today(), format="DD/MM/YYYY")

        # SOLICITANTE DINÂMICO (lista + digitar)
        st.markdown("**Solicitante**")
        solicitantes = ["(Selecionar)"] + listar_solicitantes_existentes()
        sol_sel = st.selectbox("Lista", solicitantes, key="sol_sel")
        sol_txt = st.text_input("Ou digite aqui", key="sol_txt")
        solicitante_final = sol_txt.strip() if sol_txt.strip() else ("" if sol_sel == "(Selecionar)" else sol_sel)

        obs = st.text_area("Observações")

        if st.button("✅ SALVAR", type="primary"):
            if fluig and mat:
                try:
                    sh = conectar_gsheets()
                    ws = sh.worksheet("rescisões")
                    headers = garantir_colunas_no_sheet(ws, COLUNAS_RESCISOES_MIN)

                    # ID incremental
                    try:
                        col_id_idx = headers.index("ID") + 1
                        ids = ws.col_values(col_id_idx)[1:]
                        valid_ids = [int(x) for x in ids if str(x).isdigit()]
                        nid = max(valid_ids) + 1 if valid_ids else 1
                    except:
                        nid = 1

                    dados = {
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
                        'SOLICITANTE': solicitante_final,
                        'EXCLUIR': ""
                    }

                    linha = [dados.get(col, "") for col in headers]
                    ws.append_row(linha)

                    st.cache_data.clear()
                    st.success("✅ Registro salvo!")
                    time.sleep(1)
                    st.rerun()

                except Exception as e:
                    st.error(f"Erro ao salvar: {e}")
            else:
                st.error("Preencha Fluig e Matrícula.")

    # ---------- LISTAGEM ----------
    st.title("Gerenciamento de Rescisões")

    try:
        sh = conectar_gsheets()
        ws_res = sh.worksheet("rescisões")
        df = pd.DataFrame(ws_res.get_all_records())
    except:
        df = pd.DataFrame(columns=COLUNAS_RESCISOES_MIN)

    if df.empty:
        df = pd.DataFrame(columns=COLUNAS_RESCISOES_MIN)

    df = norm_cols_upper(df)

    # Datas
    for col in ['DATA_DEMISSAO', 'DATA_PAGAMENTO']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True).dt.date

    # Limpezas
    if 'FLUIG' in df.columns:
        df['FLUIG'] = df['FLUIG'].astype(str).str.replace("'", "")
    if 'MATRICULA' in df.columns:
        df['MATRICULA'] = df['MATRICULA'].astype(str)

    # Booleanos
    bools = ['CALCULO_REALIZADO', 'DOC_ENVIADO', 'BAIXA_PAGAMENTO', 'FATURAMENTO', 'EXCLUIR']
    for b in bools:
        if b in df.columns:
            df[b] = df[b].apply(interpretar_booleano)

    # ---------- FILTROS ----------
    st.markdown("#### 🔍 Filtros")
    c1, c2, c3, c4, c5 = st.columns([1.3, 1.2, 1.6, 2.2, 1.7])

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
        opts_sol = []
        if "SOLICITANTE" in df.columns:
            opts_sol = sorted({str(x).strip() for x in df["SOLICITANTE"].dropna().astype(str) if str(x).strip()})
        f_sol = st.multiselect("Solicitante", options=opts_sol)

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

    st.caption(f"👁️ Visualizando: **{len(dfv)} registros filtrados**")

    # ---------- EDITOR ----------
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

    # ---------- AÇÕES ----------
    c_exp = st.columns(1)[0]
    with c_exp:
        # EXPORTA APENAS FILTRADO (dfv)
        dx = dfv.copy()
        if 'CALCULO_REALIZADO' in dx.columns: dx['CALCULO_REALIZADO'] = dx['CALCULO_REALIZADO'].apply(lambda x: formatar_para_texto(x, 'CALCULO'))
        if 'DOC_ENVIADO' in dx.columns: dx['DOC_ENVIADO'] = dx['DOC_ENVIADO'].apply(lambda x: formatar_para_texto(x, 'DOC'))
        if 'BAIXA_PAGAMENTO' in dx.columns: dx['BAIXA_PAGAMENTO'] = dx['BAIXA_PAGAMENTO'].apply(lambda x: formatar_para_texto(x, 'PAGTO'))
        if 'FATURAMENTO' in dx.columns: dx['FATURAMENTO'] = dx['FATURAMENTO'].apply(lambda x: formatar_para_texto(x, 'FAT'))
        if 'DATA_DEMISSAO' in dx.columns: dx['DATA_DEMISSAO'] = pd.to_datetime(dx['DATA_DEMISSAO'], errors='coerce').dt.strftime('%d/%m/%Y')
        if 'DATA_PAGAMENTO' in dx.columns: dx['DATA_PAGAMENTO'] = pd.to_datetime(dx['DATA_PAGAMENTO'], errors='coerce').dt.strftime('%d/%m/%Y')

        st.download_button("📥 Baixar Excel (Somente filtrado)", to_excel_bytes(dx, "Filtrado"), "rescissoes_filtrado.xlsx")

# ==============================================================================
# PÁGINA: ATUALIZAR BASE (FUNCIONÁRIOS)
# ==============================================================================
elif pagina == "Atualizar Base":
    st.title("Atualização de Base - Funcionários (sem apagar)")

    st.info("📌 Envie um Excel/CSV com pelo menos a coluna MATRÍCULA. O sistema fará merge e atualizará/insert sem apagar os existentes.")

    up = st.file_uploader("Upload base_funcionarios (xlsx/csv)", type=["xlsx", "csv"])
    colA, colB = st.columns(2)

    with colA:
        data_atualizacao = st.date_input("Data da atualização", date.today(), format="DD/MM/YYYY")
    with colB:
        modo = st.selectbox("Como tratar duplicados?", ["Atualizar pelo MATRÍCULA (recomendado)", "Ignorar se já existe"])

    if up:
        try:
            if up.name.lower().endswith(".xlsx"):
                df_new = pd.read_excel(up)
            else:
                df_new = pd.read_csv(up, sep=None, engine="python")

            df_new = norm_cols_upper(df_new)
            if "MATRICULA" not in df_new.columns:
                st.error("❌ Sua planilha precisa ter a coluna MATRÍCULA (MATRICULA).")
                st.stop()

            df_new["MATRICULA"] = df_new["MATRICULA"].apply(limpar_matricula)
            df_new["ATUALIZADO_EM"] = formatar_data_para_salvar(data_atualizacao)

            sh = conectar_gsheets()
            ws = sh.worksheet("base_funcionarios")
            df_old = pd.DataFrame(ws.get_all_records())
            if df_old.empty:
                df_old = pd.DataFrame(columns=df_new.columns)
            df_old = norm_cols_upper(df_old)
            if "MATRICULA" not in df_old.columns:
                df_old["MATRICULA"] = ""

            df_old["MATRICULA"] = df_old["MATRICULA"].apply(limpar_matricula)

            # Garante colunas
            todas = sorted(set(df_old.columns.tolist() + df_new.columns.tolist()))
            df_old = df_old.reindex(columns=todas, fill_value="")
            df_new = df_new.reindex(columns=todas, fill_value="")

            if modo.startswith("Atualizar"):
                # merge update/insert
                old_idx = df_old.set_index("MATRICULA")
                new_idx = df_new.set_index("MATRICULA")

                # atualiza linhas existentes
                old_idx.update(new_idx)

                # adiciona novas
                faltantes = new_idx.index.difference(old_idx.index)
                df_add = new_idx.loc[faltantes].reset_index()
                df_fin = old_idx.reset_index()
                df_fin = pd.concat([df_fin, df_add], ignore_index=True)

            else:
                # ignorar existentes
                existentes = set(df_old["MATRICULA"].astype(str))
                df_add = df_new[~df_new["MATRICULA"].astype(str).isin(existentes)]
                df_fin = pd.concat([df_old, df_add], ignore_index=True)

            df_fin = df_fin.replace([np.inf, -np.inf, np.nan], "").fillna("")
            matriz = [df_fin.columns.tolist()] + df_fin.astype(str).values.tolist()

            if st.button("✅ Aplicar atualização na base_funcionarios"):
                ws.clear()
                ws.update(matriz)
                carregar_bases.clear()
                st.cache_data.clear()
                st.success(f"✅ Base atualizada! Total agora: {len(df_fin)} linhas.")
                time.sleep(1)
                st.rerun()

            st.subheader("Prévia do que vai ser gravado")
            st.dataframe(df_fin.tail(30), use_container_width=True)

        except Exception as e:
            st.error(f"Erro ao processar arquivo: {e}")

# ==============================================================================
# PÁGINA: GESTÃO USUÁRIOS
# ==============================================================================
elif pagina == "Gestão Usuários":
    st.title("Admin - Gestão Usuários")
    sh = conectar_gsheets()
    ws_u = sh.worksheet("usuarios")

    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Novo")
        with st.form("new_user"):
            nu = st.text_input("Login")
            ns = st.text_input("Senha")
            if st.form_submit_button("Criar"):
                ws_u.append_row([nu, ns])
                st.success("✅ Criado!")
                time.sleep(0.5)
                st.rerun()

    with c2:
        st.subheader("Ativos")
        d = ws_u.get_all_records()
        if d:
            df_u = pd.DataFrame(d)
            st.dataframe(df_u, use_container_width=True)
