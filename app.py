# --- SUBSTIUA APENAS A FUNÇÃO conectar_gsheets POR ESTA ---

@st.cache_resource
def conectar_gsheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # TENTATIVA 1: Conexão via Segredos do Streamlit Cloud (NUVEM)
    if "gcp_service_account" in st.secrets:
        creds_dict = st.secrets["gcp_service_account"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        return client.open("SistemaDP_DB")

    # TENTATIVA 2: Conexão via Arquivo Local (SEU PC)
    elif os.path.exists("credenciais.json"):
        creds = ServiceAccountCredentials.from_json_keyfile_name("credenciais.json", scope)
        client = gspread.authorize(creds)
        return client.open("SistemaDP_DB")
        
    else:
        st.error("🚨 ERRO: Credenciais não encontradas (Nem segredos, nem arquivo JSON).")
        st.stop()