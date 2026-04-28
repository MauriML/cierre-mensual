"""
main.py — Backend FastAPI
Optimizador de Cierre Mensual · Estudio Contable Argentina
v4.0 — Feriados calculados automáticamente para cualquier año
"""

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import pandas as pd
import io, re, json, logging, unicodedata
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Cierre Mensual · Estudio Contable", version="4.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR      = Path(__file__).parent
UPLOAD_DIR    = BASE_DIR / "uploads"
OUTPUT_DIR    = BASE_DIR / "outputs"
CLIENTES_FILE = BASE_DIR / "clientes.json"
UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

ADMIN_PASSWORD = "estudio2025"


# ══════════════════════════════════════════════════════════════════════════════
# FERIADOS ARGENTINA — CÁLCULO AUTOMÁTICO PARA CUALQUIER AÑO
# ══════════════════════════════════════════════════════════════════════════════

def calcular_feriados(anio: int) -> set:
    """
    Calcula los feriados nacionales de Argentina para cualquier año.
    Incluye feriados fijos, inamovibles y los que se trasladan al lunes más cercano.
    Se actualiza automáticamente — no hay que tocar nada año a año.
    """
    feriados = set()

    # ── Feriados FIJOS (siempre la misma fecha) ───────────────────────────────
    fijos = [
        (1, 1),   # Año Nuevo
        (3, 24),  # Día de la Memoria
        (4, 2),   # Día del Veterano y los Caídos en Malvinas
        (5, 1),   # Día del Trabajador
        (5, 25),  # Día de la Revolución de Mayo
        (6, 20),  # Paso a la Inmortalidad del Gral. Belgrano
        (7, 9),   # Día de la Independencia
        (12, 8),  # Inmaculada Concepción de María
        (12, 25), # Navidad
    ]
    for mes, dia in fijos:
        try:
            feriados.add(date(anio, mes, dia))
        except ValueError:
            pass

    # ── Carnaval (lunes y martes, 48 días antes de Pascua) ───────────────────
    pascua = calcular_pascua(anio)
    feriados.add(pascua - timedelta(days=48))  # lunes de carnaval
    feriados.add(pascua - timedelta(days=47))  # martes de carnaval
    feriados.add(pascua - timedelta(days=2))   # Viernes Santo
    feriados.add(pascua)                        # Domingo de Pascua (opcional)

    # ── Güemes (17 de junio) ─────────────────────────────────────────────────
    feriados.add(date(anio, 6, 17))

    # ── San Martín (3er lunes de agosto) ─────────────────────────────────────
    feriados.add(tercer_lunes(anio, 8))

    # ── Día de la Raza / Respeto a la Diversidad Cultural (2do lunes oct) ────
    feriados.add(segundo_lunes(anio, 10))

    # ── Soberanía Nacional (4to lunes de noviembre) ──────────────────────────
    feriados.add(cuarto_lunes(anio, 11))

    return feriados


def calcular_pascua(anio: int) -> date:
    """Algoritmo de Butcher para calcular la fecha de Pascua."""
    a = anio % 19
    b = anio // 100
    c = anio % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    mes = (h + l - 7 * m + 114) // 31
    dia = ((h + l - 7 * m + 114) % 31) + 1
    return date(anio, mes, dia)


def primer_lunes(anio: int, mes: int) -> date:
    d = date(anio, mes, 1)
    return d + timedelta(days=(7 - d.weekday()) % 7)


def segundo_lunes(anio: int, mes: int) -> date:
    return primer_lunes(anio, mes) + timedelta(weeks=1)


def tercer_lunes(anio: int, mes: int) -> date:
    return primer_lunes(anio, mes) + timedelta(weeks=2)


def cuarto_lunes(anio: int, mes: int) -> date:
    return primer_lunes(anio, mes) + timedelta(weeks=3)


def es_dia_habil(d: date) -> bool:
    """Verifica si una fecha es día hábil (no es fin de semana ni feriado)."""
    if d.weekday() >= 5:
        return False
    feriados = calcular_feriados(d.year)
    return d not in feriados


def sumar_dias_habiles(fecha: date, dias: int) -> date:
    """Suma N días hábiles a una fecha, saltando fines de semana y feriados."""
    actual, n = fecha, 0
    while n < dias:
        actual += timedelta(days=1)
        if es_dia_habil(actual):
            n += 1
    return actual


CORRIMIENTO_IVA = {0:0, 1:0, 2:1, 3:1, 4:2, 5:2, 6:3, 7:3, 8:4, 9:4}


def calcular_vencimiento_iva(cuit: str, anio: int, mes: int) -> date:
    """
    Calcula la fecha exacta de vencimiento de IVA.
    Base: día 20 del mes siguiente + corrimiento por terminación de CUIT.
    Funciona para cualquier año automáticamente.
    """
    terminacion = int(re.sub(r"\D", "", cuit)[-1])
    corrimiento = CORRIMIENTO_IVA.get(terminacion, 0)
    if mes == 12:
        base = date(anio + 1, 1, 20)
    else:
        base = date(anio, mes + 1, 20)
    return sumar_dias_habiles(base, corrimiento)


# ══════════════════════════════════════════════════════════════════════════════
# CLIENTES
# ══════════════════════════════════════════════════════════════════════════════

def cargar_clientes() -> dict:
    if not CLIENTES_FILE.exists():
        guardar_clientes({})
        return {}
    with open(CLIENTES_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def guardar_clientes(clientes: dict) -> None:
    with open(CLIENTES_FILE, "w", encoding="utf-8") as f:
        json.dump(clientes, f, ensure_ascii=False, indent=2)
    logger.info(f"💾 {len(clientes)} clientes guardados")

def verificar_admin(password: str) -> None:
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Contraseña incorrecta")

def nombre_a_id(nombre: str) -> str:
    nfkd       = unicodedata.normalize("NFKD", nombre)
    sin_tildes = "".join(c for c in nfkd if not unicodedata.combining(c))
    limpio     = re.sub(r"[^a-z0-9\s]", "", sin_tildes.lower())
    return re.sub(r"\s+", "_", limpio.strip())[:30]

def es_cuit_valido(cuit: str) -> bool:
    cuit = re.sub(r"\D", "", str(cuit))
    if len(cuit) != 11: return False
    m  = [5, 4, 3, 2, 7, 6, 5, 4, 3, 2]
    s  = sum(int(c) * v for c, v in zip(cuit[:-1], m))
    r  = s % 11
    dv = 11 - r if r not in (0, 1) else (0 if r == 0 else 9)
    return dv == int(cuit[-1])

class ClienteNuevo(BaseModel):
    nombre: str
    cuit:   str
    pin:    str

class ClienteActualizado(BaseModel):
    nombre: Optional[str] = None
    cuit:   Optional[str] = None
    pin:    Optional[str] = None


# ══════════════════════════════════════════════════════════════════════════════
# PROCESAMIENTO DE ARCHIVOS AFIP
# ══════════════════════════════════════════════════════════════════════════════

def normalizar_monto(s: pd.Series) -> pd.Series:
    return (s.astype(str)
             .str.replace(r"[$\s]", "", regex=True)
             .str.replace(r"\.(?=\d{3})", "", regex=True)
             .str.replace(",", ".")
             .pipe(pd.to_numeric, errors="coerce")
             .fillna(0.0))

def leer_csv_afip(b: bytes) -> pd.DataFrame:
    df = pd.read_csv(io.BytesIO(b), encoding="ISO-8859-1", sep=";", dtype=str, skipinitialspace=True)
    df.columns = (df.columns.str.strip().str.upper().str.replace(" ", "_")
                  .str.replace("Á","A").str.replace("É","E").str.replace("Í","I")
                  .str.replace("Ó","O").str.replace("Ú","U").str.replace("Ñ","N"))
    rn = {
        "FECHA":"fecha", "TIPO":"tipo", "PUNTO_DE_VENTA":"pto_venta",
        "NUMERO_DESDE":"numero", "CUIT_EMISOR":"cuit", "CUIT_RECEPTOR":"cuit",
        "RAZON_SOCIAL":"razon_social", "IMPORTE_TOTAL":"total",
        "IVA_21%":"iva_21", "IVA_10,5%":"iva_105", "IMPORTE_NETO_GRAVADO":"neto"
    }
    df = df.rename(columns={k: v for k, v in rn.items() if k in df.columns})
    if "fecha" in df.columns: df["fecha"] = pd.to_datetime(df["fecha"], dayfirst=True, errors="coerce")
    if "cuit"  in df.columns: df["cuit"]  = df["cuit"].astype(str).str.replace(r"[-\s]","",regex=True).str.zfill(11)
    for c in ["total","iva_21","iva_105","neto"]:
        if c in df.columns: df[c] = normalizar_monto(df[c])
    df["clave"] = (
        df.get("cuit",      pd.Series([""]*len(df))).astype(str) + "|" +
        df.get("tipo",      pd.Series([""]*len(df))).astype(str) + "|" +
        df.get("pto_venta", pd.Series([""]*len(df))).astype(str).str.zfill(4) + "|" +
        df.get("numero",    pd.Series([""]*len(df))).astype(str).str.zfill(8)
    )
    return df

def leer_excel_cliente(b: bytes) -> pd.DataFrame:
    dr = pd.read_excel(io.BytesIO(b), header=None, nrows=10, dtype=str)
    kw = {"CUIT","FECHA","FACTURA","IMPORTE","TOTAL","IVA","NUMERO"}
    hr = 0
    for idx, row in dr.iterrows():
        if len(kw & {str(v).upper().strip() for v in row if pd.notna(v)}) >= 2:
            hr = idx; break
    df = pd.read_excel(io.BytesIO(b), header=hr, dtype=str)
    df.columns = df.columns.astype(str).str.strip().str.upper().str.replace(" ","_")
    if "FECHA" in df.columns: df["fecha"] = pd.to_datetime(df["FECHA"], dayfirst=True, errors="coerce")
    if "CUIT"  in df.columns: df["cuit"]  = df["CUIT"].astype(str).str.replace(r"[-\s]","",regex=True).str.zfill(11)
    for c in ["TOTAL","IVA","IMPORTE","NETO"]:
        if c in df.columns: df[c.lower()] = normalizar_monto(df[c])
    tc = next((c for c in df.columns if any(p in c for p in ["TIPO","COMP"])), None)
    nc = next((c for c in df.columns if any(p in c for p in ["NUMER","NRO"])), None)
    pc = next((c for c in df.columns if any(p in c for p in ["PUNTO","PTO"])), None)
    df["clave"] = (
        df.get("cuit", pd.Series([""]*len(df))).astype(str) + "|" +
        (df[tc].astype(str) if tc else pd.Series([""]*len(df))) + "|" +
        (df[pc].astype(str).str.zfill(4) if pc else pd.Series(["0000"]*len(df))) + "|" +
        (df[nc].astype(str).str.zfill(8) if nc else pd.Series([""]*len(df)))
    )
    return df

def conciliar(df_afip: pd.DataFrame, df_cli: pd.DataFrame) -> dict:
    TOL = 0.01
    if "cuit" in df_afip.columns: df_afip["cuit_valido"] = df_afip["cuit"].apply(es_cuit_valido)
    if "cuit" in df_cli.columns:  df_cli["cuit_valido"]  = df_cli["cuit"].apply(es_cuit_valido)
    dm   = pd.merge(df_afip, df_cli, on="clave", how="outer", indicator=True, suffixes=("_afip","_cli"))
    disc = []
    for _, row in dm.iterrows():
        base = {
            "cuit":        str(row.get("cuit_afip", row.get("cuit_cli", row.get("cuit","")))),
            "fecha":       str(row.get("fecha_afip", row.get("fecha_cli","")))[:10],
            "comprobante": str(row.get("tipo_afip",  row.get("tipo_cli",  row.get("tipo","")))),
        }
        if row["_merge"] == "left_only":
            disc.append({**base, "tipo_alerta":"FALTANTE_EXCEL", "emoji":"⚠️",
                "descripcion":"En AFIP pero no en Excel del cliente",
                "total_afip":float(row.get("total",0) or 0), "total_cliente":None, "diferencia":None, "prioridad":"ALTA"})
        elif row["_merge"] == "right_only":
            disc.append({**base, "tipo_alerta":"FALTANTE_AFIP", "emoji":"🚨",
                "descripcion":"En Excel pero no en AFIP — posible factura no emitida",
                "total_afip":None, "total_cliente":float(row.get("total",0) or 0), "diferencia":None, "prioridad":"CRITICA"})
        else:
            ta = float(row.get("total_afip",0) or 0)
            tc = float(row.get("total_cli",0) or 0)
            df = abs(ta - tc)
            if not row.get("cuit_valido_afip", True):
                disc.append({**base, "tipo_alerta":"CUIT_INVALIDO", "emoji":"🔴",
                    "descripcion":"CUIT no válido — verificar padrón AFIP",
                    "total_afip":ta, "total_cliente":tc, "diferencia":None, "prioridad":"CRITICA"})
            elif df > TOL:
                disc.append({**base, "tipo_alerta":"DIFERENCIA_MONTO", "emoji":"💰",
                    "descripcion":f"Diferencia de ${df:,.2f}",
                    "total_afip":round(ta,2), "total_cliente":round(tc,2),
                    "diferencia":round(df,2), "prioridad":"MEDIA" if df < 1000 else "ALTA"})
    n = len(df_afip)
    return {
        "ok":True,
        "stats":{
            "total_afip":n, "total_cliente":len(df_cli),
            "matcheados":len(dm[dm["_merge"]=="both"]),
            "discrepancias":len(disc),
            "criticas":sum(1 for d in disc if d["prioridad"]=="CRITICA"),
            "altas":sum(1 for d in disc if d["prioridad"]=="ALTA"),
            "porcentaje_ok":round((n-len(disc))/max(n,1)*100,1),
        },
        "discrepancias":disc,
        "timestamp":datetime.now().isoformat(),
    }


# ══════════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/")
def root():
    return {"mensaje":"Cierre Mensual v4.0 ✅","docs":"http://localhost:8000/docs"}

@app.get("/health")
def health():
    hoy = date.today()
    return {"status":"ok","fecha":str(hoy),"timestamp":datetime.now().isoformat()}

# ── Login ──────────────────────────────────────────────────────────────────────
@app.post("/api/login")
def login(password: str):
    verificar_admin(password)
    return {"ok":True,"mensaje":"Bienvenida ✅"}

# ── Clientes ───────────────────────────────────────────────────────────────────
@app.get("/api/clientes")
def listar(password: str):
    verificar_admin(password)
    c = cargar_clientes()
    return {"ok":True,"total":len(c),
            "clientes":[{"id":k,"nombre":v["nombre"],"cuit":v["cuit"],
                         "pin":v["pin"],"pin_hint":v["pin"][0]+"***"} for k,v in c.items()]}

@app.post("/api/clientes")
def agregar(cliente: ClienteNuevo, password: str):
    verificar_admin(password)
    if not es_cuit_valido(cliente.cuit):
        raise HTTPException(400, f"CUIT inválido: {cliente.cuit}")
    if len(cliente.pin) < 4:
        raise HTTPException(400, "El PIN debe tener al menos 4 caracteres")
    c   = cargar_clientes()
    cid = nombre_a_id(cliente.nombre)
    if cid in c:
        i = 2
        while f"{cid}_{i}" in c: i += 1
        cid = f"{cid}_{i}"
    c[cid] = {"nombre":cliente.nombre.strip(),"cuit":re.sub(r"[-\s]","",cliente.cuit),"pin":cliente.pin}
    guardar_clientes(c)
    return {"ok":True,"cliente_id":cid,"mensaje":f"'{cliente.nombre}' agregado"}

@app.put("/api/clientes/{cid}")
def actualizar(cid: str, datos: ClienteActualizado, password: str):
    verificar_admin(password)
    c = cargar_clientes()
    if cid not in c: raise HTTPException(404,"Cliente no encontrado")
    if datos.cuit and not es_cuit_valido(datos.cuit):
        raise HTTPException(400,f"CUIT inválido: {datos.cuit}")
    if datos.nombre: c[cid]["nombre"] = datos.nombre.strip()
    if datos.cuit:   c[cid]["cuit"]   = re.sub(r"[-\s]","",datos.cuit)
    if datos.pin:    c[cid]["pin"]    = datos.pin
    guardar_clientes(c)
    return {"ok":True,"mensaje":"Cliente actualizado"}

@app.delete("/api/clientes/{cid}")
def eliminar(cid: str, password: str):
    verificar_admin(password)
    c = cargar_clientes()
    if cid not in c: raise HTTPException(404,"Cliente no encontrado")
    nombre = c[cid]["nombre"]
    del c[cid]
    guardar_clientes(c)
    return {"ok":True,"mensaje":f"'{nombre}' eliminado"}

# ── Feriados (para consulta desde el frontend si se necesita) ──────────────────
@app.get("/api/feriados/{anio}")
def feriados(anio: int, password: str):
    verificar_admin(password)
    f = calcular_feriados(anio)
    return {
        "ok":True,"anio":anio,"total":len(f),
        "feriados":sorted([str(d) for d in f]),
        "pascua":str(calcular_pascua(anio)),
    }

# ── Conciliación ───────────────────────────────────────────────────────────────
@app.post("/api/conciliar")
async def conciliar_endpoint(
    afip_ventas:   UploadFile = File(...),
    afip_compras:  UploadFile = File(...),
    excel_cliente: UploadFile = File(...),
    periodo: str = "2026-04",
    password: str = "",
):
    verificar_admin(password)
    try:
        df_v  = leer_csv_afip(await afip_ventas.read())
        df_c  = leer_csv_afip(await afip_compras.read())
        df_a  = pd.concat([df_v, df_c], ignore_index=True)
        df_cl = leer_excel_cliente(await excel_cliente.read())
        res   = conciliar(df_a, df_cl)
        res["periodo"] = periodo
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        with open(OUTPUT_DIR/f"conciliacion_{periodo}_{ts}.json","w",encoding="utf-8") as f:
            json.dump(res, f, ensure_ascii=False, indent=2, default=str)
        return res
    except Exception as e:
        logger.error(f"❌ {e}")
        raise HTTPException(500, str(e))

# ── Exportar reporte Excel ─────────────────────────────────────────────────────
@app.post("/api/exportar/{cliente_id}")
async def exportar_reporte(
    cliente_id:    str,
    afip_ventas:   UploadFile = File(...),
    afip_compras:  UploadFile = File(...),
    excel_cliente: UploadFile = File(...),
    periodo: str = "2026-04",
    password: str = "",
):
    verificar_admin(password)
    try:
        clientes = cargar_clientes()
        cliente  = clientes.get(cliente_id, {"nombre": cliente_id})
        df_v  = leer_csv_afip(await afip_ventas.read())
        df_c  = leer_csv_afip(await afip_compras.read())
        df_a  = pd.concat([df_v, df_c], ignore_index=True)
        df_cl = leer_excel_cliente(await excel_cliente.read())
        res   = conciliar(df_a, df_cl)

        buf  = io.BytesIO()
        disc = pd.DataFrame(res["discrepancias"]) if res["discrepancias"] else pd.DataFrame()
        stats_df = pd.DataFrame([{
            "Cliente":       cliente["nombre"],
            "Período":       periodo,
            "Total AFIP":    res["stats"]["total_afip"],
            "Total Cliente": res["stats"]["total_cliente"],
            "Discrepancias": res["stats"]["discrepancias"],
            "Críticas":      res["stats"]["criticas"],
            "% OK":          f"{res['stats']['porcentaje_ok']}%",
            "Generado":      datetime.now().strftime("%d/%m/%Y %H:%M"),
        }])

        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            stats_df.to_excel(writer, sheet_name="Resumen", index=False)
            if not disc.empty:
                cols = ["emoji","tipo_alerta","cuit","descripcion","comprobante",
                        "total_afip","total_cliente","diferencia","prioridad"]
                cols_ok = [c for c in cols if c in disc.columns]
                disc[cols_ok].to_excel(writer, sheet_name="Discrepancias", index=False)
            df_a.drop(columns=["clave"], errors="ignore").to_excel(writer, sheet_name="AFIP", index=False)

        buf.seek(0)
        nombre_archivo = f"reporte_{cliente['nombre'].replace(' ','_')}_{periodo}.xlsx"
        return StreamingResponse(
            buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={nombre_archivo}"}
        )
    except Exception as e:
        raise HTTPException(500, str(e))

# ── Vencimientos ───────────────────────────────────────────────────────────────
@app.get("/api/vencimientos/{periodo}")
def vencimientos(periodo: str, password: str):
    verificar_admin(password)
    try:
        anio, mes = int(periodo[:4]), int(periodo[5:7])
    except:
        raise HTTPException(400, "Formato inválido. Usar YYYY-MM")

    hoy = date.today()
    c   = cargar_clientes()
    res = []

    for cid, cl in c.items():
        cuit_l = re.sub(r"\D", "", cl["cuit"])
        venc   = calcular_vencimiento_iva(cuit_l, anio, mes)
        dias   = (venc - hoy).days
        res.append({
            "cliente_id":        cid,
            "nombre":            cl["nombre"],
            "cuit":              cl["cuit"],
            "concepto":          f"IVA {mes:02d}/{anio}",
            "fecha_vencimiento": venc.strftime("%d/%m/%Y"),
            "dias_restantes":    dias,
            "urgencia":          "rojo" if dias<=2 else "amarillo" if dias<=7 else "verde",
            "vencido":           dias < 0,
        })

    return {
        "ok":True, "periodo":periodo,
        "vencimientos":sorted(res, key=lambda x: x["dias_restantes"]),
        "timestamp":datetime.now().isoformat(),
    }

# ── Validar CUIT ───────────────────────────────────────────────────────────────
@app.get("/api/validar-cuit/{cuit}")
def validar_cuit(cuit: str):
    v = es_cuit_valido(cuit)
    return {"cuit":cuit,"valido":v,
            "mensaje":"✅ CUIT válido" if v else "❌ CUIT inválido — verificar en padrón AFIP"}