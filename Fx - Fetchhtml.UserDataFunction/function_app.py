# function_app.py
import time, random, logging
import requests
import fabric.functions as fn

udf = fn.UserDataFunctions()

# ---- Sesión global (reutiliza conexiones) ----
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "F1-ETL/0.1 (+contact: your_email@example.com)",
    "Accept-Language": "en-US,en;q=0.9",
})

# ---- Configuración centralizada ----
FETCH_TIMEOUT      = 30      # seg
FETCH_RETRIES      = 3
FETCH_SLEEP_BASE   = 0.5     # seg entre requests
FETCH_JITTER_MAX   = 0.6     # aleatorio extra (0..0.6s)
FETCH_BACKOFF_STEP = 2       # 2s, 4s, 6s ...

def _fetch_html_helper(url: str) -> str:
    """Descarga HTML con backoff y rate-limit fijo."""
    for i in range(FETCH_RETRIES):
        resp = SESSION.get(url, timeout=FETCH_TIMEOUT)

        if resp.status_code == 200:
            time.sleep(FETCH_SLEEP_BASE + random.uniform(0, FETCH_JITTER_MAX))
            return resp.text

        # 429/403/503 -> espera incremental y reintenta
        if resp.status_code in (429, 403, 503):
            time.sleep(FETCH_BACKOFF_STEP * (i + 1))
            continue

        # otros códigos -> error inmediato
        resp.raise_for_status()

    raise RuntimeError(f"Failed GET {url} after {FETCH_RETRIES} tries")

# ---- UDF pública (un solo parámetro; sin underscores) ----
@udf.function()
def fetch_html(url: str) -> str:
    try:
        if not (url.startswith("http://") or url.startswith("https://")):
            raise fn.UserThrownError("URL must start with http:// or https://", {"url": url})

        return _fetch_html_helper(url)

    except requests.Timeout as te:
        raise fn.UserThrownError("Timeout while fetching URL", {"url": url, "error": str(te)})

    except requests.RequestException as re:
        # Errores HTTP/Conexión
        raise fn.UserThrownError("HTTP error while fetching URL", {"url": url, "error": str(re)})

    except Exception as ex:
        logging.exception("Unexpected error in fetch_html")
        raise fn.UserThrownError("Unhandled error fetching HTML", {"url": url, "error": str(ex)})

