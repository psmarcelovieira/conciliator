import hashlib
import os
import sqlite3
from datetime import datetime
from itertools import combinations
from typing import Iterable, List

import pandas as pd
import streamlit as st

DB_PATH = "conciliator.db"

BASE_KEY_FIELDS = [
    "data_lancamento",
    "data_efetiva",
    "evento_contabil",
    "produto_contabil",
    "valor",
    "numero_documento",
    "origem_lancamento",
    "dc",
]


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                password TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'analyst'
            );

            CREATE TABLE IF NOT EXISTS rel003_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id TEXT NOT NULL,
                record_key TEXT NOT NULL,
                subconta TEXT,
                unidade TEXT,
                data_lancamento TEXT,
                data_efetiva TEXT,
                evento_contabil TEXT,
                produto_contabil TEXT,
                numero_documento TEXT,
                origem_lancamento TEXT,
                valor REAL,
                dc TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sinaf_lancamentos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                record_key TEXT,
                ol TEXT,
                historico TEXT,
                numero_documento TEXT,
                valor REAL,
                data_lancamento TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS reconciliations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recon_group TEXT NOT NULL,
                record_key TEXT NOT NULL,
                status TEXT NOT NULL,
                method TEXT NOT NULL,
                note TEXT,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS annotations (
                record_key TEXT PRIMARY KEY,
                note TEXT NOT NULL,
                updated_by TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                action TEXT NOT NULL,
                details TEXT,
                created_at TEXT NOT NULL
            );
            """
        )

        conn.execute(
            "INSERT OR IGNORE INTO users(username, password, role) VALUES (?, ?, ?)",
            ("admin", "admin123", "admin"),
        )


def log_action(username: str, action: str, details: str = "") -> None:
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO audit_log(username, action, details, created_at) VALUES (?, ?, ?, ?)",
            (username, action, details, datetime.utcnow().isoformat()),
        )


def hash_key(values: Iterable) -> str:
    payload = "|".join([str(v).strip().lower() for v in values])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def normalize_rel003(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "Data Lançamento": "data_lancamento",
        "Data Efetiva": "data_efetiva",
        "Evento": "evento_contabil",
        "Produto": "produto_contabil",
        "Valor": "valor",
        "Documento": "numero_documento",
        "Origem": "origem_lancamento",
        "D/C": "dc",
        "Subconta": "subconta",
        "Unidade": "unidade",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    for col in BASE_KEY_FIELDS + ["subconta", "unidade"]:
        if col not in df.columns:
            df[col] = ""

    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0.0)
    df["record_key"] = df.apply(lambda x: hash_key([x[c] for c in BASE_KEY_FIELDS]), axis=1)
    return df


def normalize_sinaf(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "OL": "ol",
        "Histórico": "historico",
        "Documento": "numero_documento",
        "Valor": "valor",
        "Data Lançamento": "data_lancamento",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    for col in ["ol", "historico", "numero_documento", "valor", "data_lancamento"]:
        if col not in df.columns:
            df[col] = ""
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0.0)
    df["record_key"] = df.apply(
        lambda x: hash_key([x["data_lancamento"], x["numero_documento"], x["valor"]]), axis=1
    )
    return df


def save_rel003_snapshot(df: pd.DataFrame, username: str) -> str:
    snapshot_id = datetime.utcnow().strftime("REL003_%Y%m%d%H%M%S")
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        for _, row in df.iterrows():
            conn.execute(
                """
                INSERT INTO rel003_entries(
                    snapshot_id, record_key, subconta, unidade, data_lancamento,
                    data_efetiva, evento_contabil, produto_contabil, numero_documento,
                    origem_lancamento, valor, dc, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id,
                    row["record_key"],
                    row["subconta"],
                    row["unidade"],
                    str(row["data_lancamento"]),
                    str(row["data_efetiva"]),
                    str(row["evento_contabil"]),
                    str(row["produto_contabil"]),
                    str(row["numero_documento"]),
                    str(row["origem_lancamento"]),
                    float(row["valor"]),
                    str(row["dc"]),
                    now,
                ),
            )
    log_action(username, "upload_rel003", f"snapshot={snapshot_id}; rows={len(df)}")
    return snapshot_id


def save_sinaf(df: pd.DataFrame, username: str) -> None:
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        for _, row in df.iterrows():
            conn.execute(
                """
                INSERT INTO sinaf_lancamentos(record_key, ol, historico, numero_documento, valor, data_lancamento, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["record_key"],
                    str(row["ol"]),
                    str(row["historico"]),
                    str(row["numero_documento"]),
                    float(row["valor"]),
                    str(row["data_lancamento"]),
                    now,
                ),
            )
    log_action(username, "upload_sinaf", f"rows={len(df)}")


def get_latest_rel003() -> pd.DataFrame:
    with get_conn() as conn:
        row = conn.execute("SELECT snapshot_id FROM rel003_entries ORDER BY id DESC LIMIT 1").fetchone()
        if not row:
            return pd.DataFrame()
        snapshot_id = row["snapshot_id"]
        df = pd.read_sql_query(
            "SELECT * FROM rel003_entries WHERE snapshot_id = ?",
            conn,
            params=(snapshot_id,),
        )
    return df


def get_reconciled_keys() -> set[str]:
    with get_conn() as conn:
        rows = conn.execute("SELECT DISTINCT record_key FROM reconciliations WHERE status='conciliado'").fetchall()
    return {r["record_key"] for r in rows}


def auto_reconcile(df: pd.DataFrame, username: str) -> int:
    if df.empty:
        return 0

    reconciled = 0
    groups = []
    for _, sub_df in df.groupby("subconta"):
        credits = sub_df[sub_df["dc"].astype(str).str.upper().str.startswith("C")]
        debits = sub_df[sub_df["dc"].astype(str).str.upper().str.startswith("D")]
        for _, c in credits.iterrows():
            match = debits[debits["valor"] == c["valor"]]
            if not match.empty:
                d = match.iloc[0]
                groups.append([c["record_key"], d["record_key"]])
                debits = debits[debits["record_key"] != d["record_key"]]

    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        for grp in groups:
            recon_group = hash_key(grp + [now])
            for k in grp:
                conn.execute(
                    """
                    INSERT INTO reconciliations(recon_group, record_key, status, method, note, created_by, created_at)
                    VALUES (?, ?, 'conciliado', 'automatico', ?, ?, ?)
                    """,
                    (recon_group, k, "match 1x1 valor", username, now),
                )
                reconciled += 1
    log_action(username, "auto_reconcile", f"records={reconciled}")
    return reconciled


def suggest_matches(df: pd.DataFrame) -> pd.DataFrame:
    suggestions: List[dict] = []
    for subconta, sub_df in df.groupby("subconta"):
        pending = sub_df.copy()
        for _, row in pending.iterrows():
            target = -row["valor"] if str(row["dc"]).upper().startswith("D") else row["valor"]
            others = pending[pending["record_key"] != row["record_key"]]
            values = list(others["valor"].astype(float).values)
            keys = list(others["record_key"].values)
            for r in (1, 2, 3):
                for idxs in combinations(range(len(values)), r):
                    if abs(sum(values[i] for i in idxs) - target) < 0.001:
                        suggestions.append(
                            {
                                "subconta": subconta,
                                "record_key_base": row["record_key"],
                                "chaves_sugeridas": ", ".join(keys[i] for i in idxs),
                                "valor_alvo": target,
                                "soma_sugerida": sum(values[i] for i in idxs),
                            }
                        )
                        break
                if suggestions:
                    break
    return pd.DataFrame(suggestions)


def authenticate(username: str, password: str) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM users WHERE username = ? AND password = ?",
            (username, password),
        ).fetchone()
    return row is not None


def require_login() -> None:
    if st.session_state.get("user"):
        return

    st.title("Conciliador Contábil")
    st.subheader("Login")
    username = st.text_input("Usuário")
    password = st.text_input("Senha", type="password")
    if st.button("Entrar"):
        if authenticate(username, password):
            st.session_state["user"] = username
            log_action(username, "login", "login realizado")
            st.success("Login realizado.")
            st.rerun()
        else:
            st.error("Credenciais inválidas")
    st.stop()


def app_data_load() -> None:
    st.header("Carga de dados")
    user = st.session_state["user"]

    rel_file = st.file_uploader("Upload REL003 (CSV)", type=["csv"], key="rel")
    sinaf_file = st.file_uploader("Upload lançamentos SINAF (CSV)", type=["csv"], key="sinaf")

    col1, col2 = st.columns(2)
    with col1:
        if rel_file is not None:
            rel_df = normalize_rel003(pd.read_csv(rel_file))
            st.write("Prévia REL003")
            st.dataframe(rel_df.head(20), use_container_width=True)
            if st.button("Salvar snapshot REL003"):
                sid = save_rel003_snapshot(rel_df, user)
                st.success(f"Snapshot salvo: {sid}")

    with col2:
        if sinaf_file is not None:
            sinaf_df = normalize_sinaf(pd.read_csv(sinaf_file))
            st.write("Prévia SINAF")
            st.dataframe(sinaf_df.head(20), use_container_width=True)
            if st.button("Salvar lançamentos SINAF"):
                save_sinaf(sinaf_df, user)
                st.success("Lançamentos SINAF salvos")


def app_reconciliation() -> None:
    st.header("Conciliação")
    user = st.session_state["user"]
    df = get_latest_rel003()
    if df.empty:
        st.info("Sem snapshot REL003 carregado.")
        return

    reconciled_keys = get_reconciled_keys()
    pending_df = df[~df["record_key"].isin(reconciled_keys)]
    conciliated_df = df[df["record_key"].isin(reconciled_keys)]

    c1, c2, c3 = st.columns(3)
    c1.metric("Registros totais", len(df))
    c2.metric("Conciliados", len(conciliated_df))
    c3.metric("Pendentes", len(pending_df))

    if st.button("Executar conciliação automática"):
        qtd = auto_reconcile(pending_df, user)
        st.success(f"{qtd} registros conciliados automaticamente")
        st.rerun()

    tabs = st.tabs(["Pendentes", "Conciliados", "Sugestões", "Anotações", "Exportar acertos"])

    with tabs[0]:
        st.dataframe(pending_df, use_container_width=True)
        opts = pending_df["record_key"].tolist()
        selected = st.multiselect("Selecionar chaves para conciliar manualmente", options=opts)
        if st.button("Conciliar selecionados manualmente") and selected:
            recon_group = hash_key(selected + [datetime.utcnow().isoformat()])
            now = datetime.utcnow().isoformat()
            with get_conn() as conn:
                for key in selected:
                    conn.execute(
                        """
                        INSERT INTO reconciliations(recon_group, record_key, status, method, note, created_by, created_at)
                        VALUES (?, ?, 'conciliado', 'manual', ?, ?, ?)
                        """,
                        (recon_group, key, "conciliação manual", user, now),
                    )
            log_action(user, "manual_reconcile", f"records={len(selected)}")
            st.success("Registros migrados para conciliados")
            st.rerun()

    with tabs[1]:
        st.dataframe(conciliated_df, use_container_width=True)

    with tabs[2]:
        if st.button("Gerar sugestões"):
            s = suggest_matches(pending_df)
            if s.empty:
                st.info("Sem sugestões com o algoritmo atual.")
            else:
                st.dataframe(s, use_container_width=True)

    with tabs[3]:
        key = st.text_input("record_key para anotação")
        note = st.text_area("Anotação")
        if st.button("Salvar anotação") and key and note:
            now = datetime.utcnow().isoformat()
            with get_conn() as conn:
                conn.execute(
                    """
                    INSERT INTO annotations(record_key, note, updated_by, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(record_key) DO UPDATE SET
                        note=excluded.note,
                        updated_by=excluded.updated_by,
                        updated_at=excluded.updated_at
                    """,
                    (key, note, user, now),
                )
            log_action(user, "annotation", f"record_key={key}")
            st.success("Anotação salva")
        with get_conn() as conn:
            ann = pd.read_sql_query("SELECT * FROM annotations ORDER BY updated_at DESC", conn)
            st.dataframe(ann, use_container_width=True)

    with tabs[4]:
        with get_conn() as conn:
            adj = pd.read_sql_query(
                "SELECT * FROM reconciliations WHERE status='conciliado' ORDER BY created_at DESC",
                conn,
            )
        st.dataframe(adj, use_container_width=True)
        if not adj.empty:
            st.download_button(
                "Baixar arquivo de acertos (CSV)",
                adj.to_csv(index=False).encode("utf-8"),
                file_name="acertos_sinaf.csv",
                mime="text/csv",
            )


def app_audit() -> None:
    st.header("Trilha de auditoria")
    with get_conn() as conn:
        logs = pd.read_sql_query("SELECT * FROM audit_log ORDER BY created_at DESC", conn)
    st.dataframe(logs, use_container_width=True)


def main() -> None:
    st.set_page_config(page_title="Conciliador", layout="wide")
    init_db()
    require_login()

    st.sidebar.title("Menu")
    page = st.sidebar.radio("Navegação", ["Carga de dados", "Conciliação", "Auditoria"])
    if st.sidebar.button("Logout"):
        log_action(st.session_state["user"], "logout", "logout realizado")
        st.session_state.pop("user")
        st.rerun()

    if page == "Carga de dados":
        app_data_load()
    elif page == "Conciliação":
        app_reconciliation()
    else:
        app_audit()


if __name__ == "__main__":
    main()
