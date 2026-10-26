# -*- coding: utf-8 -*-
"""
recolector_opciones.py (v1.1) — base de datos diaria de opciones + IV Rank propio + paper trading de straddles
==============================================================================================================
POR QUÉ: yfinance solo entrega la cadena de opciones ACTUAL (sin IV histórica). La única forma de saber si un straddle
habría ganado (movimiento realizado vs prima pagada) o de calcular un IV Rank real es guardar las cadenas todos los días.

ARQUITECTURA (SQLite, un archivo: opciones.db)
  capturas          una fila por (fecha, hora, ticker): spot, calidad (OK / BAJA / ERROR), nº de contratos y de cotizaciones válidas
  contratos         cada contrato capturado (±banda de strikes, dte <= max): bid, ask, último, volumen, OI, IV de Yahoo, cot_valida
  atm               un straddle ATM por captura y vencimiento: mid, ask, bid, movimiento implícito (% del spot), spread, IV; y la
                    etiqueta `ref` ('7d', '30d', '60d') del vencimiento más cercano a cada plazo constante (para series comparables)
  earnings          fechas de reporte conocidas por ticker (pasadas y futuras)
  paper_straddles   simulación: compra a ASK, valor de salida a BID, P&L %
  meta              versión del esquema
  Vistas: v_capturas_oficiales (última captura OK de cada ticker y día), v_serie_atm (serie diaria por plazo constante + días a earnings)
  Idempotente: UNIQUE(fecha, ticker, hora) y PRIMARY KEYs; repetir una corrida el mismo minuto no duplica nada.

CUÁNDO CORRERLO: entre ~9:45 y ~15:55 hora de Nueva York (mejor cerca de las 15:30, antes del cierre). Fuera de horario Yahoo suele
devolver bid/ask = 0 y la captura queda marcada BAJA (no se usa en series ni en el paper trading). Los fines de semana se omite.

DRIVE: SQLite sobre Google Drive puede fallar (bloqueos). Se trabaja en /content/opciones.db y al terminar se copia a
  /content/drive/My Drive/Trading_Reports/opciones.db (conservando opciones_prev.db como respaldo).

USO:
    python recolector_opciones.py                         # captura los 10 símbolos habituales
    python recolector_opciones.py INTC AMD --max-dte 75
    python recolector_opciones.py --estado                # qué hay en la base y cuánto falta para un IV Rank fiable
    python recolector_opciones.py --paper-abrir INTC AMD --ref 30d     # abre straddles de papel (compra al ASK)
    python recolector_opciones.py --paper-estado          # valuación a BID con la última captura
    python recolector_opciones.py --paper-cerrar 1 2      # cierra posiciones (venta al BID)
    python recolector_opciones.py --exportar              # CSV de la serie ATM
    python recolector_opciones.py --selftest              # prueba offline
En Jupyter/Colab:   from recolector_opciones import capturar_todo; capturar_todo(["INTC", "AMD"])
AVISO: herramienta de investigación, no asesoría financiera. Comprar opciones puede perder el 100% de la prima.
"""
import argparse
import os
import shutil
import sqlite3
import sys
import time
import zoneinfo
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import json
import os

ID_ARCHIVO_DRIVE = "1zMNVZiRj8a4xwPDuwU4PF9QzVgzB36lB"

VERSION_ESQUEMA = 1
NY = zoneinfo.ZoneInfo("America/New_York")
TICKERS_DEFECTO = ["NVDA", "AMD", "INTC", "MU", "AVGO", "AAPL", "TSLA", "PLTR", "GOOG", "META"]
RAIZ_DRIVE = "/content/drive/My Drive"
CARPETA_DRIVE = os.path.join(RAIZ_DRIVE, "Trading_Reports")
NOMBRE_DB = "opciones.db"
DB_LOCAL = "/content/opciones.db"
REFS = {"7d": (7, 4, 12), "30d": (30, 20, 40), "60d": (60, 45, 75)}      # (objetivo, dte mín, dte máx)
MIN_CALIDAD = 0.60                                                      # fracción mínima de straddles ATM con cotización válida
IV_RANK_MIN_OBS = 60


# --------------------------------------------------------------------------------------------------
# Funciones de Acceso directo a Google Drive
# --------------------------------------------------------------------------------------------------

def conectar_drive_puro():
    """Se conecta a la API de Google Drive usando las credenciales del robot."""
    json_creds = None
    
    # 1. Intentar leer desde Colab Secrets
    try:
        from google.colab import userdata
        json_creds = userdata.get("GDRIVE_CREDENTIALS")
    except ImportError:
        pass
        
    # 2. Intentar leer desde variables de entorno (GitHub Actions)
    if not json_creds:
        json_creds = os.environ.get("GDRIVE_CREDENTIALS")
        
    if not json_creds:
        print("  AVISO: No se encontraron credenciales de Drive.")
        return None
   
    info = json.loads(json_creds)
    creds = service_account.Credentials.from_service_account_info(info)
    return build('drive', 'v3', credentials=creds)

def traer_de_drive(local: str, remoto: str = None) -> None:
    """Sobrescribe la función original de la línea 124 para usar la API de Drive."""
    servicio = conectar_drive_puro()
    if not servicio: return
    
    print("  Descargando opciones.db desde Google Drive vía API...")
    solicitud = servicio.files().get_media(fileId=ID_ARCHIVO_DRIVE)
    with open(local, 'wb') as f:
        descarga = MediaIoBaseDownload(f, solicitud)
        hecho = False
        while not hecho:
            _, hecho = descarga.next_chunk()

def subir_a_drive(local: str, remoto: str = None) -> None:
    """Sobrescribe la función original de la línea 129 para usar la API de Drive."""
    servicio = conectar_drive_puro()
    if not servicio: return
    
    print("  Subiendo opciones.db actualizada a Google Drive vía API...")
    media = MediaFileUpload(local, mimetype='application/x-sqlite3', resumable=True)
    servicio.files().update(fileId=ID_ARCHIVO_DRIVE, media_body=media).execute()

# --------------------------------------------------------------------------------------------------
# Esquema
# --------------------------------------------------------------------------------------------------
ESQUEMA = """
CREATE TABLE IF NOT EXISTS meta (clave TEXT PRIMARY KEY, valor TEXT);
CREATE TABLE IF NOT EXISTS capturas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fecha TEXT NOT NULL, hora_ny TEXT NOT NULL, ticker TEXT NOT NULL,
    spot REAL, mercado_abierto INTEGER, n_vencimientos INTEGER DEFAULT 0, n_contratos INTEGER DEFAULT 0,
    n_straddles INTEGER DEFAULT 0, n_straddles_validos INTEGER DEFAULT 0,
    calidad TEXT NOT NULL DEFAULT 'ERROR' CHECK (calidad IN ('OK','BAJA','ERROR')), nota TEXT,
    UNIQUE (fecha, ticker, hora_ny)
);
CREATE INDEX IF NOT EXISTS ix_capturas_tf ON capturas (ticker, fecha);
CREATE TABLE IF NOT EXISTS contratos (
    captura_id INTEGER NOT NULL REFERENCES capturas(id),
    contrato TEXT NOT NULL, vencimiento TEXT NOT NULL, dte INTEGER, tipo TEXT CHECK (tipo IN ('C','P')),
    strike REAL, bid REAL, ask REAL, ultimo REAL, volumen REAL, oi REAL, iv REAL, itm INTEGER, cot_valida INTEGER,
    PRIMARY KEY (captura_id, contrato)
);
CREATE INDEX IF NOT EXISTS ix_contratos_c ON contratos (contrato);
CREATE TABLE IF NOT EXISTS atm (
    captura_id INTEGER NOT NULL REFERENCES capturas(id),
    vencimiento TEXT NOT NULL, dte INTEGER, ref TEXT, strike REAL, call_contrato TEXT, put_contrato TEXT,
    call_bid REAL, call_ask REAL, put_bid REAL, put_ask REAL,
    straddle_mid REAL, straddle_ask REAL, straddle_bid REAL, mov_implicito_pct REAL, spread_pct REAL,
    iv_call REAL, iv_put REAL, iv_atm REAL, cot_valida INTEGER,
    PRIMARY KEY (captura_id, vencimiento)
);
CREATE TABLE IF NOT EXISTS earnings (
    ticker TEXT NOT NULL, fecha TEXT NOT NULL, fuente TEXT, capturado TEXT, hora TEXT, PRIMARY KEY (ticker, fecha)
);
CREATE TABLE IF NOT EXISTS paper_straddles (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT NOT NULL, ref TEXT, abierto_fecha TEXT NOT NULL,
    vencimiento TEXT NOT NULL, strike REAL, call_contrato TEXT NOT NULL, put_contrato TEXT NOT NULL,
    spot_entrada REAL, prima_entrada REAL NOT NULL, mov_implicito_pct REAL, iv_entrada REAL, dias_a_earnings INTEGER,
    cerrado_fecha TEXT, valor_salida REAL, pnl_pct REAL, nota TEXT
);
CREATE VIEW IF NOT EXISTS v_capturas_oficiales AS
    SELECT c.* FROM capturas c
    WHERE c.calidad = 'OK' AND c.id = (SELECT MAX(c2.id) FROM capturas c2
                                       WHERE c2.ticker = c.ticker AND c2.fecha = c.fecha AND c2.calidad = 'OK');
CREATE VIEW IF NOT EXISTS v_serie_atm AS
    SELECT c.fecha, c.ticker, c.spot, a.ref, a.vencimiento, a.dte, a.strike, a.straddle_mid, a.mov_implicito_pct,
           a.spread_pct, a.iv_atm,
           (SELECT CAST(MIN(julianday(e.fecha) - julianday(c.fecha)) AS INTEGER) FROM earnings e
             WHERE e.ticker = c.ticker AND e.fecha >= c.fecha) AS dias_a_earnings
    FROM v_capturas_oficiales c JOIN atm a ON a.captura_id = c.id
    WHERE a.ref IS NOT NULL AND a.cot_valida = 1;
"""


def abrir(ruta: str) -> sqlite3.Connection:
    con = sqlite3.connect(ruta)
    con.execute("PRAGMA foreign_keys = ON")
    con.executescript(ESQUEMA)
    if "hora" not in [r[1] for r in con.execute("PRAGMA table_info(earnings)")]:      # migración v1.0 -> v1.1 (hora del reporte)
        con.execute("ALTER TABLE earnings ADD COLUMN hora TEXT")
    con.execute("INSERT OR IGNORE INTO meta VALUES ('version_esquema', ?)", (str(VERSION_ESQUEMA),))
    con.commit()
    return con


# --------------------------------------------------------------------------------------------------
# Drive (trabajo local + copia atómica)
# --------------------------------------------------------------------------------------------------
def _montar_drive() -> bool:
    if os.path.isdir(RAIZ_DRIVE):
        return True
    try:
        from google.colab import drive as _d                  # solo existe en Colab
    except ImportError:
        return False
    try:
        _d.mount("/content/drive")
    except Exception as e:
        print(f"AVISO recolector: no se pudo montar Drive ({type(e).__name__}: {e}); la base quedará solo en local")
    return os.path.isdir(RAIZ_DRIVE)

'''
def traer_de_drive(local: str, remoto: str) -> None:
    if os.path.exists(remoto) and (not os.path.exists(local) or os.path.getmtime(remoto) > os.path.getmtime(local)):
        shutil.copy2(remoto, local)


def subir_a_drive(local: str, remoto: str) -> None:
    os.makedirs(os.path.dirname(remoto), exist_ok=True)
    if os.path.exists(remoto):
        shutil.copy2(remoto, os.path.join(os.path.dirname(remoto), "opciones_prev.db"))      # respaldo del día anterior
    tmp = remoto + ".tmp"
    shutil.copy2(local, tmp)
    os.replace(tmp, remoto)                                                                 # reemplazo atómico
'''

def rutas_db(db: Optional[str] = None) -> Tuple[str, Optional[str]]:
    """(base de trabajo, copia en Drive o None)."""
    if db:
        return db, None
    if _montar_drive():
        return DB_LOCAL, os.path.join(CARPETA_DRIVE, NOMBRE_DB)
    return NOMBRE_DB, None


# --------------------------------------------------------------------------------------------------
# Captura
# --------------------------------------------------------------------------------------------------
def _valida(bid: Any, ask: Any) -> np.ndarray:
    b, a = pd.to_numeric(pd.Series(bid), errors="coerce"), pd.to_numeric(pd.Series(ask), errors="coerce")
    return ((b > 0) & (a > 0) & (a >= b)).values


def _con_reintento(f: Callable[[], Any], intentos: int = 3, pausa: float = 1.0) -> Any:
    ult: Optional[Exception] = None
    for k in range(intentos):
        try:
            return f()
        except Exception as e:
            ult = e
            time.sleep(pausa * (k + 1))
    raise ult  # type: ignore[misc]


def _spot(t: Any) -> Optional[float]:
    for f in (lambda: float(t.fast_info["last_price"]), lambda: float(t.history(period="1d")["Close"].iloc[-1])):
        try:
            v = f()
            if np.isfinite(v) and v > 0:
                return v
        except Exception:
            continue
    return None


def fila_atm(calls: pd.DataFrame, puts: pd.DataFrame, spot: float) -> Optional[Dict[str, Any]]:
    """Straddle ATM: el strike más cercano al spot con cotización válida en AMBAS patas (a <=3% del spot); si no hay, el más cercano (inválido)."""
    m = calls.merge(puts, on="strike", suffixes=("_c", "_p"))
    if m.empty:
        return None
    m = m.assign(valido=_valida(m["bid_c"], m["ask_c"]) & _valida(m["bid_p"], m["ask_p"]), dist=(m["strike"] - spot).abs()).sort_values("dist")
    cand = m[m["valido"] & (m["dist"] <= 0.03 * spot)]
    r = cand.iloc[0] if len(cand) else m.iloc[0]
    ok = bool(len(cand))
    cmid, pmid = (r["bid_c"] + r["ask_c"]) / 2.0, (r["bid_p"] + r["ask_p"]) / 2.0
    mid, ask, bid = (cmid + pmid, r["ask_c"] + r["ask_p"], r["bid_c"] + r["bid_p"]) if ok else (np.nan, np.nan, np.nan)
    ivs = [v for v in (r["iv_c"], r["iv_p"]) if pd.notna(v) and v > 0.01]
    return {"strike": float(r["strike"]), "call_contrato": r["contrato_c"], "put_contrato": r["contrato_p"],
            "call_bid": float(r["bid_c"]), "call_ask": float(r["ask_c"]), "put_bid": float(r["bid_p"]), "put_ask": float(r["ask_p"]),
            "straddle_mid": float(mid), "straddle_ask": float(ask), "straddle_bid": float(bid),
            "mov_implicito_pct": float(mid / spot * 100.0) if ok else np.nan,
            "spread_pct": float((ask - bid) / mid * 100.0) if ok and mid > 0 else np.nan,
            "iv_call": float(r["iv_c"]) if pd.notna(r["iv_c"]) else np.nan, "iv_put": float(r["iv_p"]) if pd.notna(r["iv_p"]) else np.nan,
            "iv_atm": float(np.mean(ivs)) if ivs else np.nan, "cot_valida": int(ok)}


def asignar_refs(atm: List[Dict[str, Any]]) -> None:
    """Marca con 'ref' el vencimiento más cercano a cada plazo constante (7d/30d/60d), prefiriendo cotizaciones válidas."""
    for ref, (obj, lo, hi) in REFS.items():
        c = [a for a in atm if lo <= a["dte"] <= hi]
        if c:
            min(c, key=lambda a: (1 - a["cot_valida"], abs(a["dte"] - obj)))["ref"] = ref


def _norm(df: pd.DataFrame, tipo: str, spot: float, banda: float, dte: int, exp: str) -> pd.DataFrame:
    d = df.copy()
    d = d[(d["strike"] / spot - 1.0).abs() <= banda]
    d = d.assign(tipo=tipo, vencimiento=exp, dte=dte)
    for c in ("bid", "ask", "lastPrice", "volume", "openInterest", "impliedVolatility"):
        d[c] = pd.to_numeric(d.get(c), errors="coerce")
    d["cot_valida"] = _valida(d["bid"], d["ask"]).astype(int)
    d["itm"] = d["inTheMoney"].astype(float).fillna(0).astype(int) if "inTheMoney" in d else 0
    return d


def actualizar_earnings(con: sqlite3.Connection, ticker: str, yf: Any, hoy: str) -> int:
    ya = con.execute("SELECT 1 FROM earnings WHERE ticker=? AND capturado=? LIMIT 1", (ticker, hoy)).fetchone()
    sin_hora = con.execute("SELECT 1 FROM earnings WHERE ticker=? AND hora IS NULL LIMIT 1", (ticker,)).fetchone()
    if ya and not sin_hora:
        return 0
    try:
        ed = yf.Ticker(ticker).get_earnings_dates(limit=12)
        if ed is None or len(ed) == 0:
            return 0
        por_fecha: Dict[str, Optional[str]] = {}
        for t_ in pd.DatetimeIndex(ed.index):
            if pd.notna(t_):
                por_fecha[pd.Timestamp(t_).strftime("%Y-%m-%d")] = pd.Timestamp(t_).strftime("%H:%M")     # hora local del reporte (NY)
        con.executemany("INSERT OR REPLACE INTO earnings (ticker, fecha, fuente, capturado, hora) VALUES (?,?,?,?,?)",
                        [(ticker, f, "yfinance", hoy, h) for f, h in sorted(por_fecha.items())])
        con.commit()
        return len(por_fecha)
    except Exception:
        return 0


def capturar(con: sqlite3.Connection, ticker: str, yf: Any, ahora: Optional[pd.Timestamp] = None, max_dte: int = 60,
             banda: float = 0.12, pausa: float = 0.4, forzar: bool = False) -> Dict[str, Any]:
    ahora = ahora if ahora is not None else pd.Timestamp.now(tz=NY)
    fecha, hora = ahora.strftime("%Y-%m-%d"), ahora.strftime("%Y-%m-%d %H:%M")
    abierto = int(ahora.weekday() < 5 and (9, 30) <= (ahora.hour, ahora.minute) < (16, 0))
    if ahora.weekday() >= 5 and not forzar:
        return {"ticker": ticker, "estado": "OMITIDA (fin de semana; usa --forzar si quieres capturar de todos modos)"}
    cur = con.execute("INSERT OR IGNORE INTO capturas (fecha, hora_ny, ticker, mercado_abierto, calidad) VALUES (?,?,?,?, 'ERROR')",
                      (fecha, hora, ticker, abierto))
    if cur.rowcount == 0:
        return {"ticker": ticker, "estado": "DUPLICADA (ya capturada en ese minuto)"}
    cid = cur.lastrowid
    con.commit()
    try:
        t = yf.Ticker(ticker)
        spot = _spot(t)
        if spot is None:
            raise RuntimeError("sin precio spot")
        exps = list(_con_reintento(lambda: t.options) or [])
        hoy_ts = pd.Timestamp(fecha)
        atm: List[Dict[str, Any]] = []
        n_cont = n_val = 0
        for exp in exps:
            dte = int((pd.Timestamp(exp) - hoy_ts).days)
            if dte < 0 or dte > max_dte:
                continue
            ch = _con_reintento(lambda: t.option_chain(exp))
            calls, puts = _norm(ch.calls, "C", spot, banda, dte, exp), _norm(ch.puts, "P", spot, banda, dte, exp)
            todo = pd.concat([calls, puts], ignore_index=True)
            if todo.empty:
                continue
            con.executemany(
                "INSERT OR IGNORE INTO contratos VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [(cid, r.contractSymbol, exp, dte, r.tipo, float(r.strike), r.bid, r.ask, r.lastPrice, r.volume, r.openInterest,
                  r.impliedVolatility, int(r.itm), int(r.cot_valida)) for r in todo.itertuples()])
            n_cont += len(todo)
            n_val += int(todo["cot_valida"].sum())
            c_ = calls[["strike", "contractSymbol", "bid", "ask", "impliedVolatility"]].rename(
                columns={"contractSymbol": "contrato", "impliedVolatility": "iv"})
            p_ = puts[["strike", "contractSymbol", "bid", "ask", "impliedVolatility"]].rename(
                columns={"contractSymbol": "contrato", "impliedVolatility": "iv"})
            f = _atm_desde(c_, p_, spot)
            if f:
                f.update({"vencimiento": exp, "dte": dte, "ref": None})
                atm.append(f)
            time.sleep(pausa)
        asignar_refs(atm)
        con.executemany("INSERT OR REPLACE INTO atm VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        [(cid, a["vencimiento"], a["dte"], a["ref"], a["strike"], a["call_contrato"], a["put_contrato"], a["call_bid"],
                          a["call_ask"], a["put_bid"], a["put_ask"], a["straddle_mid"], a["straddle_ask"], a["straddle_bid"],
                          a["mov_implicito_pct"], a["spread_pct"], a["iv_call"], a["iv_put"], a["iv_atm"], a["cot_valida"]) for a in atm])
        rel = [a for a in atm if 4 <= a["dte"] <= 75] or atm
        frac = (sum(a["cot_valida"] for a in rel) / len(rel)) if rel else 0.0
        calidad = "OK" if (frac >= MIN_CALIDAD and n_cont > 0) else "BAJA"
        nota = None if calidad == "OK" else (f"cotizaciones válidas {frac:.0%} < {MIN_CALIDAD:.0%}"
                                             f"{' (mercado cerrado: Yahoo suele devolver bid/ask = 0)' if not abierto else ''}")
        con.execute("UPDATE capturas SET spot=?, n_vencimientos=?, n_contratos=?, n_straddles=?, n_straddles_validos=?, calidad=?, nota=? WHERE id=?",
                    (spot, len(atm), n_cont, len(atm), sum(a["cot_valida"] for a in atm), calidad, nota, cid))
        con.commit()
        ref30 = next((a for a in atm if a["ref"] == "30d"), None)
        return {"ticker": ticker, "estado": calidad, "spot": spot, "vencimientos": len(atm), "contratos": n_cont, "ref30": ref30, "nota": nota}
    except Exception as e:
        con.execute("UPDATE capturas SET calidad='ERROR', nota=? WHERE id=?", (f"{type(e).__name__}: {e}", cid))
        con.commit()
        return {"ticker": ticker, "estado": f"ERROR ({type(e).__name__}: {e})"}


def _atm_desde(c: pd.DataFrame, p: pd.DataFrame, spot: float) -> Optional[Dict[str, Any]]:
    """Adapta (strike, contrato, bid, ask, iv) de calls y puts al formato de fila_atm."""
    cc = c.rename(columns={"contrato": "contrato_c", "bid": "bid_c", "ask": "ask_c", "iv": "iv_c"})
    pp = p.rename(columns={"contrato": "contrato_p", "bid": "bid_p", "ask": "ask_p", "iv": "iv_p"})
    return fila_atm(cc, pp, spot)


def capturar_todo(tickers: Optional[List[str]] = None, db: Optional[str] = None, max_dte: int = 60, banda: float = 0.12,
                  forzar: bool = False, yf: Any = None, ahora: Optional[pd.Timestamp] = None, pausa: float = 0.4) -> List[Dict[str, Any]]:
    tickers = [t.upper() for t in (tickers or TICKERS_DEFECTO)]
    if yf is None:
        import yfinance as yf                                  # noqa: F811
    local, remoto = rutas_db(db)
    if remoto:
        traer_de_drive(local, remoto)
    con = abrir(local)
    ahora = ahora if ahora is not None else pd.Timestamp.now(tz=NY)
    res: List[Dict[str, Any]] = []
    print(f"\nCAPTURA DE OPCIONES | {ahora:%Y-%m-%d %H:%M} NY | base: {remoto or local}")
    if not (ahora.weekday() < 5 and (9, 45) <= (ahora.hour, ahora.minute) < (15, 59)):
        print("  AVISO: fuera del horario recomendado (9:45–15:55 NY). Yahoo suele devolver bid/ask = 0 y la captura saldrá BAJA.")
    for t in tickers:
        actualizar_earnings(con, t, yf, ahora.strftime("%Y-%m-%d"))
        r = capturar(con, t, yf, ahora, max_dte, banda, pausa, forzar)
        res.append(r)
        if r.get("ref30"):
            a = r["ref30"]
            print(f"  {t:5s} spot {r['spot']:9.2f} | {r['vencimientos']:2d} venc | 30d: exp {a['vencimiento']} K {a['strike']:g} "
                  f"straddle {a['straddle_mid']:.2f} ({a['mov_implicito_pct']:.1f}% del spot) IV {a['iv_atm'] * 100:.0f}% spread {a['spread_pct']:.1f}% | {r['estado']}")
        else:
            print(f"  {t:5s} {r['estado']}{' | ' + r['nota'] if r.get('nota') else ''}")
    con.close()
    if remoto:
        subir_a_drive(local, remoto)
        print(f"  Base copiada a Drive: {remoto}")
    return res


# --------------------------------------------------------------------------------------------------
# IV Rank y estado
# --------------------------------------------------------------------------------------------------
def iv_rank(con: sqlite3.Connection, ticker: str, ref: str = "30d", ventana: int = 252) -> Tuple[Optional[float], int]:
    """Percentil (0-100) de la IV ATM de hoy entre las últimas `ventana` capturas OK. Devuelve (rank|None, n observaciones)."""
    s = pd.read_sql_query("SELECT fecha, iv_atm FROM v_serie_atm WHERE ticker=? AND ref=? AND iv_atm IS NOT NULL ORDER BY fecha",
                          con, params=(ticker, ref))["iv_atm"].tail(ventana)
    n = len(s)
    if n < IV_RANK_MIN_OBS:
        return None, n
    return float((s <= s.iloc[-1]).mean() * 100.0), n


def estado(con: sqlite3.Connection, ruta: str = "") -> pd.DataFrame:
    n = con.execute("SELECT COUNT(*), MIN(fecha), MAX(fecha) FROM capturas").fetchone()
    print(f"\nESTADO DE LA BASE {ruta}\n  capturas {n[0]} | del {n[1]} al {n[2]} | contratos {con.execute('SELECT COUNT(*) FROM contratos').fetchone()[0]:,} "
          f"| straddles ATM {con.execute('SELECT COUNT(*) FROM atm').fetchone()[0]:,}"
          f" | tamaño {os.path.getsize(ruta) / 1e6:.1f} MB" if ruta and os.path.exists(ruta) else f"\nESTADO DE LA BASE\n  capturas {n[0]}")
    calid = pd.read_sql_query("SELECT calidad, COUNT(*) n FROM capturas GROUP BY calidad", con)
    if len(calid):
        print("  calidad: " + ", ".join(f"{r.calidad} {r.n}" for r in calid.itertuples()))
    filas = []
    for (t,) in con.execute("SELECT DISTINCT ticker FROM capturas ORDER BY ticker").fetchall():
        dias = con.execute("SELECT COUNT(DISTINCT fecha) FROM v_capturas_oficiales WHERE ticker=?", (t,)).fetchone()[0]
        ult = con.execute("SELECT MAX(fecha) FROM v_capturas_oficiales WHERE ticker=?", (t,)).fetchone()[0]
        rk, nobs = iv_rank(con, t)
        ult_iv = con.execute("SELECT iv_atm, mov_implicito_pct, dias_a_earnings FROM v_serie_atm WHERE ticker=? AND ref='30d' ORDER BY fecha DESC LIMIT 1", (t,)).fetchone()
        filas.append({"ticker": t, "días_OK": dias, "última": ult, "obs_IV30d": nobs, "IV_rank_30d": rk if rk is not None else np.nan,
                      "IV_30d_%": ult_iv[0] * 100 if ult_iv and ult_iv[0] else np.nan, "mov_impl_30d_%": ult_iv[1] if ult_iv else np.nan,
                      "días_a_earnings": ult_iv[2] if ult_iv else None,
                      "falta_para_rank_fiable": max(0, IV_RANK_MIN_OBS - nobs)})
    df = pd.DataFrame(filas)
    if not df.empty:
        pd.set_option("display.width", 220, "display.float_format", lambda v: f"{v:,.1f}")
        print(df.to_string(index=False))
        print(f"  (IV Rank solo se muestra con >= {IV_RANK_MIN_OBS} observaciones; ideal >= 252 = un año.)")
    return df


# --------------------------------------------------------------------------------------------------
# Paper trading
# --------------------------------------------------------------------------------------------------
def _cotizacion(con: sqlite3.Connection, contrato: str, hasta: Optional[str] = None) -> Optional[Tuple[str, float, float]]:
    """Última cotización VÁLIDA del contrato en una captura oficial: (fecha, bid, ask)."""
    q = ("SELECT c.fecha, k.bid, k.ask FROM contratos k JOIN v_capturas_oficiales c ON c.id = k.captura_id "
         "WHERE k.contrato=? AND k.cot_valida=1" + (" AND c.fecha<=?" if hasta else "") + " ORDER BY c.fecha DESC LIMIT 1")
    r = con.execute(q, (contrato, hasta) if hasta else (contrato,)).fetchone()
    return (r[0], r[1], r[2]) if r else None


def paper_abrir(con: sqlite3.Connection, ticker: str, ref: str = "30d", fecha: Optional[str] = None, nota: str = "") -> Optional[int]:
    q = ("SELECT c.fecha, c.spot, a.vencimiento, a.strike, a.call_contrato, a.put_contrato, a.call_ask, a.put_ask, a.mov_implicito_pct, a.iv_atm, "
         "(SELECT CAST(MIN(julianday(e.fecha)-julianday(c.fecha)) AS INTEGER) FROM earnings e WHERE e.ticker=c.ticker AND e.fecha>=c.fecha) "
         "FROM v_capturas_oficiales c JOIN atm a ON a.captura_id=c.id WHERE c.ticker=? AND a.ref=? AND a.cot_valida=1 "
         + ("AND c.fecha=? " if fecha else "") + "ORDER BY c.fecha DESC LIMIT 1")
    r = con.execute(q, (ticker, ref, fecha) if fecha else (ticker, ref)).fetchone()
    if not r:
        print(f"  {ticker}: no hay captura OK con straddle {ref} válido{' en ' + fecha if fecha else ''}")
        return None
    prima = float(r[6] + r[7])                                                    # se compra al ASK de ambas patas
    cur = con.execute("INSERT INTO paper_straddles (ticker, ref, abierto_fecha, vencimiento, strike, call_contrato, put_contrato, spot_entrada, "
                      "prima_entrada, mov_implicito_pct, iv_entrada, dias_a_earnings, nota) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                      (ticker, ref, r[0], r[2], r[3], r[4], r[5], r[1], prima, r[8], r[9], r[10], nota))
    con.commit()
    print(f"  #{cur.lastrowid} {ticker} {ref}: compra straddle K {r[3]:g} exp {r[2]} al ASK = {prima:.2f} ({prima / r[1] * 100:.1f}% del spot) el {r[0]}")
    return int(cur.lastrowid)


def paper_estado(con: sqlite3.Connection) -> pd.DataFrame:
    filas = []
    for p in con.execute("SELECT id, ticker, ref, abierto_fecha, vencimiento, strike, call_contrato, put_contrato, prima_entrada, "
                         "cerrado_fecha, valor_salida, pnl_pct, iv_entrada, dias_a_earnings FROM paper_straddles ORDER BY id").fetchall():
        (pid, t, ref, fa, ven, k, cc, pc, prima, fc, vs, pnl, iv0, dte_e) = p
        if fc:
            estado_, f_, val, pn = "CERRADA", fc, vs, pnl
        else:
            qc, qp = _cotizacion(con, cc), _cotizacion(con, pc)
            if qc and qp and qc[0] == qp[0]:
                estado_, f_, val = "ABIERTA", qc[0], qc[1] + qp[1]                 # valuación al BID de ambas patas
                pn = (val / prima - 1.0) * 100.0
            else:
                estado_, f_, val, pn = "ABIERTA (sin cotización)", None, np.nan, np.nan
        filas.append({"id": pid, "ticker": t, "ref": ref, "abierta": fa, "vence": ven, "strike": k, "prima": prima, "fecha_val": f_,
                      "valor_bid": val, "pnl_%": pn, "estado": estado_, "IV_entrada_%": (iv0 or np.nan) * 100 if iv0 else np.nan,
                      "días_a_earnings_entrada": dte_e})
    df = pd.DataFrame(filas)
    if df.empty:
        print("  (sin posiciones de papel)")
    else:
        pd.set_option("display.width", 220, "display.float_format", lambda v: f"{v:,.2f}")
        print(df.to_string(index=False))
        cer = df[df["estado"] == "CERRADA"]["pnl_%"]
        if len(cer):
            print(f"  Cerradas: {len(cer)} | P&L medio {cer.mean():+.1f}% | ganadoras {(cer > 0).mean() * 100:.0f}% | (n pequeño: anecdótico)")
    return df


def paper_cerrar(con: sqlite3.Connection, pid: int, fecha: Optional[str] = None) -> Optional[float]:
    p = con.execute("SELECT call_contrato, put_contrato, prima_entrada, cerrado_fecha FROM paper_straddles WHERE id=?", (pid,)).fetchone()
    if not p:
        print(f"  #{pid}: no existe")
        return None
    if p[3]:
        print(f"  #{pid}: ya estaba cerrada el {p[3]}")
        return None
    qc, qp = _cotizacion(con, p[0], fecha), _cotizacion(con, p[1], fecha)
    if not (qc and qp and qc[0] == qp[0]):
        print(f"  #{pid}: sin cotización válida de ambas patas en la misma captura (¿venció? ¿falta capturar?)")
        return None
    val = float(qc[1] + qp[1])                                                    # se vende al BID
    pnl = (val / p[2] - 1.0) * 100.0
    con.execute("UPDATE paper_straddles SET cerrado_fecha=?, valor_salida=?, pnl_pct=? WHERE id=?", (qc[0], val, pnl, pid))
    con.commit()
    print(f"  #{pid}: cerrada el {qc[0]} | entrada {p[2]:.2f} -> salida (BID) {val:.2f} | P&L {pnl:+.1f}%")
    return pnl


def exportar(con: sqlite3.Connection, carpeta: str = ".") -> str:
    ruta = os.path.join(carpeta, "opciones_serie_atm.csv")
    pd.read_sql_query("SELECT * FROM v_serie_atm ORDER BY ticker, ref, fecha", con).to_csv(ruta, index=False)
    print(f"  Serie ATM exportada: {ruta}")
    return ruta


# --------------------------------------------------------------------------------------------------
# Autoprueba offline (yfinance simulado)
# --------------------------------------------------------------------------------------------------
class _Cad:
    def __init__(self, calls, puts):
        self.calls, self.puts = calls, puts


class _FakeTicker:
    """Cadena sintética: spot 100.4; vencimientos a 5, 12, 33, 40 y 70 días; strikes 80..120; precios con valor temporal gaussiano."""
    spot = 100.4
    escala = 1.0               # multiplica los precios (para simular que la IV sube)
    ceros = False              # True = bid/ask en 0 (mercado cerrado)
    dtes = (5, 12, 33, 40, 70)
    hoy = pd.Timestamp("2026-10-20")       # día de la captura (para calcular el valor temporal)
    base = pd.Timestamp("2026-10-20")      # los vencimientos son FIJOS (no se mueven con el día)

    def __init__(self, sym): self.sym = sym
    @property
    def fast_info(self): return {"last_price": self.spot}
    @property
    def options(self): return tuple((self.base + pd.Timedelta(days=d)).strftime("%Y-%m-%d") for d in self.dtes)

    def history(self, **k): return pd.DataFrame({"Close": [self.spot]})

    def get_earnings_dates(self, limit=12):
        return pd.DataFrame({"x": [1, 2]}, index=pd.DatetimeIndex(["2026-10-29 16:05", "2026-07-23 16:05"], tz="America/New_York"))

    def option_chain(self, exp):
        dte = (pd.Timestamp(exp) - self.hoy).days
        ks = np.arange(80, 121, 1.0)
        tv = 0.4 * 100 * 0.5 * np.sqrt(dte / 365) * np.exp(-((ks / self.spot - 1) ** 2) / (2 * 0.06 ** 2)) * self.escala
        def lado(tipo):
            intr = np.maximum(self.spot - ks, 0) if tipo == "C" else np.maximum(ks - self.spot, 0)
            px = intr + tv
            bid, ask = (np.zeros_like(px), np.zeros_like(px)) if self.ceros else (px * 0.97, px * 1.03)
            if not self.ceros:
                bid[ks == 100] = 0.0                     # el strike 100 sin bid en el vencimiento más corto: obliga a usar el 101
                if dte != 5: bid[ks == 100] = px[ks == 100] * 0.97
            sym = [f"{self.sym}{pd.Timestamp(exp).strftime('%y%m%d')}{tipo}{int(k * 1000):08d}" for k in ks]
            return pd.DataFrame({"contractSymbol": sym, "strike": ks, "bid": bid, "ask": ask, "lastPrice": px, "volume": 10.0,
                                 "openInterest": 100.0, "impliedVolatility": 0.5, "inTheMoney": (ks < self.spot) if tipo == "C" else (ks > self.spot)})
        return _Cad(lado("C"), lado("P"))


def _selftest() -> None:
    import tempfile, types
    yf = types.SimpleNamespace(Ticker=_FakeTicker)
    tmp = tempfile.mkdtemp()
    ruta = os.path.join(tmp, "o.db")
    # (a) esquema idempotente
    abrir(ruta).close(); con = abrir(ruta)
    assert con.execute("SELECT valor FROM meta WHERE clave='version_esquema'").fetchone()[0] == "1"
    # (b) captura en horario: calidad OK, contratos dentro de la banda, un ATM por vencimiento <= max_dte, refs 7d/30d
    t1 = pd.Timestamp("2026-10-20 15:30", tz=NY)
    r = capturar(con, "TEST", yf, t1, max_dte=60, banda=0.12, pausa=0)
    assert r["estado"] == "OK" and r["vencimientos"] == 4, r                       # 5, 12, 33 y 40 días (70 queda fuera)
    ks = [x[0] for x in con.execute("SELECT DISTINCT strike FROM contratos").fetchall()]
    assert min(ks) >= 100.4 * 0.88 and max(ks) <= 100.4 * 1.12, (min(ks), max(ks))
    refs = dict(con.execute("SELECT ref, dte FROM atm WHERE ref IS NOT NULL").fetchall())
    assert refs == {"7d": 5, "30d": 33}, refs
    a7 = con.execute("SELECT strike, cot_valida FROM atm WHERE ref='7d'").fetchone()
    assert a7 == (101.0, 1), a7                                                    # el 100 no tiene bid a 5d -> usa 101
    a30 = con.execute("SELECT strike, straddle_mid, straddle_ask, straddle_bid, mov_implicito_pct, spread_pct, iv_atm FROM atm WHERE ref='30d'").fetchone()
    assert a30[0] == 100.0 and abs(a30[1] - (a30[2] + a30[3]) / 2) < 1e-9 and abs(a30[4] - a30[1] / 100.4 * 100) < 1e-9 and abs(a30[6] - 0.5) < 1e-9, a30
    # (c) idempotencia: misma captura en el mismo minuto no duplica
    assert capturar(con, "TEST", yf, t1, pausa=0)["estado"].startswith("DUPLICADA")
    assert con.execute("SELECT COUNT(*) FROM capturas").fetchone()[0] == 1
    # (d) mercado cerrado / bid=ask=0 -> BAJA, y NO entra a las vistas oficiales
    _FakeTicker.ceros = True
    r0 = capturar(con, "TEST", yf, pd.Timestamp("2026-10-20 20:10", tz=NY), pausa=0)
    _FakeTicker.ceros = False
    assert r0["estado"] == "BAJA" and "mercado cerrado" in r0["nota"], r0
    assert con.execute("SELECT COUNT(*) FROM v_capturas_oficiales").fetchone()[0] == 1
    # (e) fin de semana: se omite salvo --forzar
    assert capturar(con, "TEST", yf, pd.Timestamp("2026-10-24 12:00", tz=NY), pausa=0)["estado"].startswith("OMITIDA")
    # (f) earnings + vista con días a earnings (29-oct está a 9 días del 20-oct)
    assert actualizar_earnings(con, "TEST", yf, "2026-10-20") == 2
    assert con.execute("SELECT hora FROM earnings WHERE ticker='TEST' AND fecha='2026-10-29'").fetchone()[0] == "16:05"
    assert actualizar_earnings(con, "TEST", yf, "2026-10-20") == 0                                  # ya actualizado hoy y con hora: no repite
    viejo = sqlite3.connect(os.path.join(tmp, "v10.db"))                                              # base creada con el esquema v1.0 (sin hora)
    viejo.execute("CREATE TABLE earnings (ticker TEXT NOT NULL, fecha TEXT NOT NULL, fuente TEXT, capturado TEXT, PRIMARY KEY (ticker, fecha))")
    viejo.execute("INSERT INTO earnings VALUES ('OLD','2026-10-29','yfinance','2026-10-20')"); viejo.commit(); viejo.close()
    mig = abrir(os.path.join(tmp, "v10.db"))
    assert "hora" in [r[1] for r in mig.execute("PRAGMA table_info(earnings)")] and mig.execute("SELECT hora FROM earnings").fetchone()[0] is None
    assert actualizar_earnings(mig, "OLD", yf, "2026-10-20") == 2                                     # filas sin hora: se vuelve a consultar aunque sea el mismo día
    assert mig.execute("SELECT hora FROM earnings WHERE fecha='2026-10-29'").fetchone()[0] == "16:05"; mig.close()
    dte_e = con.execute("SELECT dias_a_earnings FROM v_serie_atm WHERE ticker='TEST' AND ref='30d'").fetchone()[0]
    assert dte_e == 9, dte_e
    # (g) paper trading: compra al ASK, valuación al BID al día siguiente con la IV/precios más altos
    pid = paper_abrir(con, "TEST", "30d", nota="prueba")
    prima = con.execute("SELECT prima_entrada FROM paper_straddles WHERE id=?", (pid,)).fetchone()[0]
    assert abs(prima - a30[2]) < 1e-9, (prima, a30)
    _FakeTicker.hoy = pd.Timestamp("2026-10-21"); _FakeTicker.escala = 1.4
    capturar(con, "TEST", yf, pd.Timestamp("2026-10-21 15:30", tz=NY), pausa=0)
    est = paper_estado(con)
    esperado = None
    cc, pc = con.execute("SELECT call_contrato, put_contrato FROM paper_straddles WHERE id=?", (pid,)).fetchone()
    q = _cotizacion(con, cc), _cotizacion(con, pc)
    assert q[0][0] == "2026-10-21" and q[1][0] == "2026-10-21"
    esperado = (q[0][1] + q[1][1]) / prima - 1.0
    assert abs(est.loc[0, "pnl_%"] / 100 - esperado) < 1e-9 and est.loc[0, "estado"] == "ABIERTA", est
    pn = paper_cerrar(con, pid)
    assert abs(pn / 100 - esperado) < 1e-9 and paper_cerrar(con, pid) is None                    # no se cierra dos veces
    _FakeTicker.hoy = pd.Timestamp("2026-10-20"); _FakeTicker.escala = 1.0
    # (h) IV Rank: exige >= 60 observaciones; con 100 días de IV creciente, hoy es el máximo (rank 100)
    assert iv_rank(con, "TEST") == (None, 2)
    capt = con.execute("SELECT id FROM capturas WHERE calidad='OK' ORDER BY id LIMIT 1").fetchone()[0]
    for k in range(100):
        f = (pd.Timestamp("2026-01-05") + pd.Timedelta(days=k)).strftime("%Y-%m-%d")
        con.execute("INSERT INTO capturas (fecha, hora_ny, ticker, spot, calidad) VALUES (?,?,?,?, 'OK')", (f, f + " 15:30", "XYZ", 50.0))
        cid = con.execute("SELECT last_insert_rowid()").fetchone()[0]
        con.execute("INSERT INTO atm (captura_id, vencimiento, dte, ref, strike, straddle_mid, mov_implicito_pct, spread_pct, iv_atm, cot_valida) "
                    "VALUES (?,?,?,?,?,?,?,?,?,1)", (cid, "2027-01-01", 30, "30d", 50.0, 3.0, 6.0, 3.0, 0.30 + 0.002 * k))
    con.commit()
    rk, n = iv_rank(con, "XYZ")
    assert n == 100 and rk == 100.0, (rk, n)
    estado(con, ruta)
    exportar(con, tmp)
    con.close()
    # (i) copia a Drive: reemplazo atómico y respaldo del día anterior
    drive = os.path.join(tmp, "My Drive", "Trading_Reports", "opciones.db")
    subir_a_drive(ruta, drive)
    assert os.path.exists(drive) and not os.path.exists(drive + ".tmp")
    subir_a_drive(ruta, drive)
    assert os.path.exists(os.path.join(os.path.dirname(drive), "opciones_prev.db"))
    local2 = os.path.join(tmp, "otro.db")
    traer_de_drive(local2, drive)
    assert os.path.exists(local2) and abrir(local2).execute("SELECT COUNT(*) FROM capturas").fetchone()[0] > 100
    print("\nOK  recolector_opciones: esquema, captura/ATM/refs, idempotencia, calidad BAJA, fin de semana, earnings, paper trading, IV Rank y copia a Drive")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        _selftest()
    else:
        ap = argparse.ArgumentParser(description="Recolector diario de opciones + paper trading de straddles")
        ap.add_argument("tickers", nargs="*")
        ap.add_argument("--db", default=None)
        ap.add_argument("--max-dte", type=int, default=60)
        ap.add_argument("--banda", type=float, default=0.12)
        ap.add_argument("--forzar", action="store_true")
        ap.add_argument("--estado", action="store_true")
        ap.add_argument("--exportar", action="store_true")
        ap.add_argument("--paper-abrir", action="store_true")
        ap.add_argument("--paper-estado", action="store_true")
        ap.add_argument("--paper-cerrar", action="store_true")
        ap.add_argument("--ref", default="30d", choices=list(REFS))
        a, _ = ap.parse_known_args()                               # tolera '-f kernel.json' de Jupyter
        palabras = [t for t in a.tickers if t.replace("-", "").replace(".", "").isalnum()]
        if a.estado or a.exportar or a.paper_abrir or a.paper_estado or a.paper_cerrar:
            local, remoto = rutas_db(a.db)
            if remoto:
                traer_de_drive(local, remoto)
            con = abrir(local)
            if a.estado:
                estado(con, local)
            if a.exportar:
                exportar(con, os.path.dirname(remoto or local) or ".")
            if a.paper_abrir:
                for t in palabras:
                    paper_abrir(con, t.upper(), a.ref)
            if a.paper_cerrar:
                for x in palabras:
                    paper_cerrar(con, int(x))
            if a.paper_estado or a.paper_abrir or a.paper_cerrar:
                paper_estado(con)
            con.close()
            if remoto and (a.paper_abrir or a.paper_cerrar):
                subir_a_drive(local, remoto)
        else:
            capturar_todo(palabras or None, a.db, a.max_dte, a.banda, a.forzar)
