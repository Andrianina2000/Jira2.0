# ===================== sync_excel_jira.py =====================
import hashlib
import datetime
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urlencode

# === CONFIG ===
EXCEL_PATH  = "Releases-Board.xlsx"
SHEET_NAME  = "ALL DATA"

API_URL     = "http://127.0.0.1:8000/releases"   # ton FastAPI
API_KEY     = "my_secret_key"

# (Lecture Jira en read-only, inchangé)
JIRA_BASE   = "https://jira-test-andrianina.atlassian.net"
JIRA_EMAIL  = "andrianinarabary3@gmail.com"
JIRA_TOKEN  = "JIRA_API_TOKEN"
PREFERRED_PROJECT = None

auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_TOKEN)
HDR_JSON = {"Accept": "application/json"}
HDR_JSON_POST = {"Accept": "application/json", "Content-Type": "application/json"}


# =============== UTIL: conversion datetime -> str =================
def _convert_datetimes_to_str(df: pd.DataFrame) -> pd.DataFrame:
    def convert_value(v):
        if isinstance(v, (pd.Timestamp, datetime.datetime, datetime.date, datetime.time)):
            return str(v)
        return v
    return df.applymap(convert_value)


# ===================== PHASE A : Excel -> API =====================
def send_excel_to_api():
    # 1) lire Excel (on ne touche ni aux noms ni à l'ordre des colonnes)
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)

    # ----- (A) Préparer Category vs Scope, sans réordonner le reste -----
    cols = list(df.columns)
    lower_to_orig = {str(c).strip().lower(): c for c in cols}
    scope_col    = lower_to_orig.get("scope")      # ex: "Scope"
    category_col = lower_to_orig.get("category")   # ex: "Category"

    # - si Category existe ET Scope existe -> compléter Category avec Scope quand Category est vide
    if category_col and scope_col:
        df[category_col] = df[category_col].where(
            df[category_col].notna() & (df[category_col].astype(str) != ""),
            df[scope_col]
        )
    # - si Scope existe ET Category n'existe pas -> insérer Category juste APRES Scope
    elif scope_col and not category_col:
        insert_pos = cols.index(scope_col) + 1
        df.insert(insert_pos, "Category", df[scope_col])

    # ----- (B) Générer un __id stable par ligne -----
    # Si une colonne candidate existe déjà, on la réutilise.
    lower_to_orig = {str(c).strip().lower(): c for c in df.columns}  # refresh après insert éventuel
    candidate = None
    for name in ("__id", "externalid", "id", "jira key", "key"):
        if name in lower_to_orig:
            candidate = lower_to_orig[name]
            break

    if candidate and candidate != "__id":
        df["__id"] = df[candidate].astype(str)
    elif "__id" not in df.columns:
        # Sinon on construit un ID déterministe à partir de colonnes stables
        app_col  = lower_to_orig.get("application")
        ver_col  = lower_to_orig.get("version")
        scope_or_cat = lower_to_orig.get("category") or lower_to_orig.get("scope")

        def make_id(row):
            parts = [
                str(row.get(app_col, "")) if app_col else "",
                str(row.get(ver_col, "")) if ver_col else "",
                str(row.get(scope_or_cat, "")) if scope_or_cat else "",
            ]
            base = "|".join(p.strip() for p in parts)
            h = hashlib.sha1(base.encode("utf-8")).hexdigest()[:10]
            return f"{(parts[0] or 'x')}-{(parts[1] or 'na')}-{h}"

        df["__id"] = df.apply(make_id, axis=1)

    # Unicité (si jamais)
    if df["__id"].duplicated().any():
        dup_index = df["__id"].duplicated(keep=False)
        df.loc[dup_index, "__id"] = (
            df.loc[dup_index, "__id"]
            + "-"
            + df.loc[dup_index].groupby("__id").cumcount().astype(str)
        )

    # 2) conversion des dates/heures en strings (pour JSON propre)
    df = _convert_datetimes_to_str(df)

    # 3) logs: on vérifie visuellement que l'ordre n'a pas bougé
    print("🧭 Ordre colonnes (1-based) :")
    for i, c in enumerate(df.columns, start=1):
        print(f"{i:>2} -> {c}")
    if len(df) > 0:
        print("🕒 Exemple 1re ligne:", {k: df.iloc[0][k] for k in df.columns})

    # 4) payload & POST
    payload = df.fillna("").to_dict(orient="records")
    print(f"📦 {len(payload)} lignes prêtes à être envoyées")

    try:
        r = requests.post(
            API_URL,
            json=payload,
            headers={"Authorization": f"Bearer {API_KEY}"},
            timeout=30,
        )
        r.raise_for_status()
        print(f"✅ Données envoyées ({len(payload)} lignes). Réponse: {r.json()}")
    except requests.RequestException as e:
        print(f"❌ Erreur API: {e}")


# ===================== PHASE B : Jira (lecture seule) =====================
def list_project_keys():
    url = f"{JIRA_BASE}/rest/api/3/project/search"
    r = requests.get(url, headers=HDR_JSON, auth=auth, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Projet: {r.status_code} - {r.text}")
    values = r.json().get("values", [])
    keys = [p.get("key") for p in values if p.get("key")]
    return keys

def resolve_project_key():
    keys = list_project_keys()
    if not keys:
        raise RuntimeError("Aucun projet accessible (vérifie tes droits).")
    if PREFERRED_PROJECT and PREFERRED_PROJECT in keys:
        print(f"ℹ️ Projet forcé: {PREFERRED_PROJECT}")
        return PREFERRED_PROJECT
    if PREFERRED_PROJECT and PREFERRED_PROJECT not in keys:
        print(f"ℹ️ Projet '{PREFERRED_PROJECT}' introuvable. Projets: {', '.join(keys)}")
    print(f"ℹ️ Projet utilisé: {keys[0]}")
    return keys[0]

def build_bounded_jql(project_key, days=30):
    return (
        f'project = {project_key} '
        f'AND issuetype in standardIssueTypes() '
        f'AND updated >= -{days}d '
        f'ORDER BY created DESC'
    )

def _extract_and_print_issues(data):
    issues = []
    if "results" in data:
        results = data.get("results", [])
        issues = results[0].get("issues", []) if results else []
    else:
        issues = data.get("issues", [])
    if not issues:
        print("⚠️ Aucun résultat trouvé.")
        return []
    for it in issues:
        key = it.get("key", "—")
        fields = it.get("fields", {}) or {}
        summary = fields.get("summary", "—")
        status = (fields.get("status") or {}).get("name", "—")
        assignee = (fields.get("assignee") or {}).get("displayName", "—")
        print(f"📋 {key} | {summary} | {status} | {assignee}")
    return issues

def fetch_jira_issues(jql=None, max_results=25, days=30):
    project_key = resolve_project_key()
    query = jql or build_bounded_jql(project_key, days=days)
    base_url = f"{JIRA_BASE}/rest/api/3/search/jql"

    params = {
        "jql": query,
        "maxResults": max_results,
        "fields": "summary,status,assignee,created,updated"
    }
    url_get = f"{base_url}?{urlencode(params)}"

    print(f"🔎 JQL: {query}")
    print(f"🌐 GET: {url_get}")

    r = requests.get(url_get, headers=HDR_JSON, auth=auth, timeout=30)
    if r.status_code == 200:
        return _extract_and_print_issues(r.json())

    # Retry avec période plus courte si message "unbound"
    if r.status_code == 400 and ("non liée" in r.text or "unbound" in r.text.lower()):
        query = build_bounded_jql(project_key, days=min(7, days))
        params["jql"] = query
        url_get2 = f"{base_url}?{urlencode(params)}"
        print(f"🔁 Retry JQL (7j): {query}")
        r2 = requests.get(url_get2, headers=HDR_JSON, auth=auth, timeout=30)
        if r2.status_code == 200:
            return _extract_and_print_issues(r2.json())
        else:
            print(f"❌ Erreur Jira (GET resserré): {r2.status_code} - {r2.text}")

    # Fallback POST
    url_post = f"{JIRA_BASE}/rest/api/3/search/jql"
    payload = {
        "jql": query,
        "startAt": 0,
        "maxResults": max_results,
        "fields": ["summary", "status", "assignee", "created", "updated"]
    }
    print(f"📮 POST: {url_post} | payload.jql='{query}'")
    r3 = requests.post(url_post, json=payload, headers=HDR_JSON_POST, auth=auth, timeout=30)
    if r3.status_code == 200:
        return _extract_and_print_issues(r3.json())

    print(f"❌ Erreur Jira (GET): {r.status_code} - {r.text}")
    print(f"❌ Erreur Jira (POST): {r3.status_code} - {r3.text}")
    return []


# ===================== MAIN =====================
def main():
    print("=== 🔄 Envoi des données Excel vers API ===")
    send_excel_to_api()
    print("\n=== 🔍 Lecture des issues Jira (read-only) ===")
    fetch_jira_issues()

if __name__ == "__main__":
    main()
