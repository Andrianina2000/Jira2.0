from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any
import re
import uvicorn

# =====================
# CONFIG
# =====================
API_KEY = "my_secret_key"
app = FastAPI(title="ALL DATA API", version="1.5")

# CORS (ouvrir pour tests; en prod, restreindre)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],            # ex prod: ["https://*.atlassian.net"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mémoire process (simple)
DATA_STORAGE: List[Dict[str, Any]] = []

# Facultatif : limiter les colonnes éditables (None = toutes)
EDITABLE_COLUMNS: Optional[set] = None
# Exemple : EDITABLE_COLUMNS = {"Category", "STATUS", "Prod"}


# =====================
# Helpers
# =====================
def _auth_or_401(authorization: Optional[str]):
    if authorization != f"Bearer {API_KEY}":
        raise HTTPException(status_code=401, detail="Unauthorized")

def _row_id(row: Dict[str, Any]) -> str:
    """Retourne l'ID de la ligne, normalisé."""
    rid = row.get("__id") or row.get("ExternalID") or row.get("ID") or row.get("id") or ""
    return str(rid).strip()

def normalize_id(rid: str) -> str:
    """Trim + normalisation légère."""
    return (rid or "").strip()

def strip_suffix_num(rid: str) -> Optional[str]:
    """Si l'ID se termine par -<nombre>, renvoie la base sans suffixe, sinon None."""
    m = re.match(r"^(.*)-(\d+)$", rid.strip())
    return m.group(1).strip() if m else None

def _attach_ids(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Ajoute __id si absent (fallback row-{i})."""
    out: List[Dict[str, Any]] = []
    for i, r in enumerate(rows):
        if not isinstance(r, dict):
            raise HTTPException(status_code=400, detail="Each item must be an object")
        rid = _row_id(r) or f"row-{i}"
        r2 = dict(r)
        r2["__id"] = rid
        out.append(r2)
    return out

def find_row_by_id(rid: str) -> Optional[Dict[str, Any]]:
    """Cherche une ligne par ID exact, puis sans suffixe -N si besoin."""
    rid = normalize_id(rid)
    # 1) exact
    for row in DATA_STORAGE:
        if _row_id(row) == rid:
            return row
    # 2) sans suffixe -N (tolérance)
    base = strip_suffix_num(rid)
    if base:
        for row in DATA_STORAGE:
            if _row_id(row) == base:
                return row
    return None


# =====================
# ENDPOINTS
# =====================
@app.get("/health")
def health():
    return {"ok": True, "rows": len(DATA_STORAGE)}

# POST: ingérer toutes les lignes
@app.post("/releases")
async def receive_releases(request: Request, authorization: Optional[str] = Header(None)):
    """Réception depuis ton script Python/Make (liste d'objets)."""
    _auth_or_401(authorization)

    payload = await request.json()
    if not isinstance(payload, list):
        raise HTTPException(status_code=400, detail="Expected a list of releases")

    DATA_STORAGE.clear()
    DATA_STORAGE.extend(_attach_ids(payload))
    return {"message": f"{len(DATA_STORAGE)} lignes reçues."}

# GET: JSON brut pour Jira (gadget)
@app.get("/releases.json", response_class=JSONResponse)
async def get_releases_json():
    return JSONResponse(content=DATA_STORAGE)

# GET: liste des IDs (debug)
@app.get("/releases/ids", response_class=JSONResponse)
def list_ids():
    return [_row_id(r) for r in DATA_STORAGE]

# GET: une ligne par id (debug)
@app.get("/releases/{rid}", response_class=JSONResponse)
def get_row(rid: str):
    row = find_row_by_id(rid)
    if row is not None:
        return row
    known = list_ids()[:5]
    raise HTTPException(status_code=404, detail=f"Row '{rid}' not found. Known sample: {known}")

# PATCH: modifier une cellule
@app.patch("/releases/{rid}", response_class=JSONResponse)
async def patch_cell(
    rid: str,
    request: Request,
    authorization: Optional[str] = Header(None),
):
    """
    Modifie une cellule d'une ligne.
    Body attendu: {"field": "NomDeColonne", "value": "..."}
    """
    _auth_or_401(authorization)

    body = await request.json()
    field = (body.get("field") or "").strip()
    value = body.get("value")

    if not field:
        raise HTTPException(status_code=400, detail="Missing 'field'")
    if EDITABLE_COLUMNS and field not in EDITABLE_COLUMNS:
        raise HTTPException(status_code=403, detail=f"Field '{field}' not editable")

    row = find_row_by_id(rid)
    if row is None:
        known = list_ids()[:5]
        raise HTTPException(status_code=404, detail=f"Row '{rid}' not found. Known sample: {known}")

    # on autorise la création du champ si absent
    row[field] = value
    return {"ok": True, "id": _row_id(row), "field": field, "value": value}

# (optionnel) Rebuild des IDs en place si tu as chargé des données sans __id
@app.post("/releases/rebuild-ids", response_class=JSONResponse)
def rebuild_ids(authorization: Optional[str] = Header(None)):
    _auth_or_401(authorization)
    for i, row in enumerate(DATA_STORAGE):
        rid = _row_id(row) or f"row-{i}"
        row["__id"] = rid
    return {"ok": True, "rows": len(DATA_STORAGE)}

# HTML (debug visuel)
@app.get("/releases", response_class=HTMLResponse)
async def get_releases_html():
    if not DATA_STORAGE:
        return HTMLResponse("<h3 style='font-family:Arial;padding:20px;'>Aucune donnée disponible.</h3>")

    cols = list(DATA_STORAGE[0].keys())
    # s'assurer que __id est visible en 1re colonne pour debug
    if "__id" in cols:
        cols = ["__id"] + [c for c in cols if c != "__id"]

    table_header = "".join(f"<th>{c}</th>" for c in cols)
    table_rows = "".join(
        "<tr>" + "".join(f"<td>{(row.get(c, '') if row.get(c, '') is not None else '')}</td>" for c in cols) + "</tr>"
        for row in DATA_STORAGE
    )

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="utf-8" />
      <title>Tableau ALL DATA</title>
      <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
      <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
      <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
      <style>
        body {{ font-family: Arial; margin: 20px; background: #121212; color: #eee; }}
        table {{ width:100%; border-collapse:collapse; }}
        th,td {{ padding:8px; text-align:left; border:1px solid #333; }}
        th {{ background:#333; }}
        a {{ color:#8ab4f8; }}
      </style>
    </head>
    <body>
      <h2>📊 Données Excel (ALL DATA)</h2>
      <table id="dataTable" class="display">
        <thead><tr>{table_header}</tr></thead>
        <tbody>{table_rows}</tbody>
      </table>
      <script>
        $(function(){{ $('#dataTable').DataTable(); }});
      </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html)

# =====================
# MAIN
# =====================
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
