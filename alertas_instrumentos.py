import pandas as pd
import requests
import io
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# === CONFIGURACIÓN ===
EXCEL_URL = os.getenv("EXCEL_URL")

DIAS_AVISO = 15
CORREOS_DESTINO = [
    "wilderalberto2000@gmail.com",
    "tecnicodeservicios@valserindustriales.com"
]

# === CONFIGURACIÓN DE CORREO (GMAIL) ===
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")   


# ----------------- FUNCIONES -----------------

def descargar_excel_bytes(url):
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception("No se pudo descargar el archivo Excel.")
    return r.content


def encontrar_encabezados_y_bloques(df_raw):
    encabezado_tokens = ["IDENTIFICACIÓN", "EQUIPO", "INSTRUMENTO", "FABRICANTE"]
    posibles_tipos = ["PLANTA", "VST2", "VST 2", "VST3", "VST 3", "VST-2", "VST-3"]

    header_rows = []
    for idx, row in df_raw.iterrows():
        row_text = " ".join([str(x).upper() for x in row.fillna("").values])
        if any(tok in row_text for tok in encabezado_tokens):
            header_rows.append(idx)

    if not header_rows:
        raise Exception("No se encontraron filas de encabezado en la hoja.")

    bloques = []

    for i, hr in enumerate(header_rows):
        start = hr
        end = header_rows[i+1] if i+1 < len(header_rows) else len(df_raw)

        titulo = None

        # 1. Buscar título EN LA FILA anterior aunque venga de celda combinada
        fila_superior = df_raw.iloc[hr-1].fillna("").astype(str).str.upper().tolist()
        texto_fila = " ".join(fila_superior)

        for p in posibles_tipos:
            if p in texto_fila:
                titulo = p.replace(" ", "").replace("-", "").upper()
                break

        # 2. Si no lo encontró, buscar 10 filas hacia arriba (combos grandes)
        if not titulo:
            for j in range(max(0, hr-10), hr):
                fila = " ".join(
                    df_raw.iloc[j].fillna("").astype(str).str.upper().tolist()
                )
                for p in posibles_tipos:
                    if p in fila:
                        titulo = p.replace(" ", "").replace("-", "").upper()
                        break
                if titulo:
                    break

        bloques.append({
            "header_row": hr,
            "start": start,
            "end": end,
            "titulo": titulo
        })

    return bloques


def construir_df_bloque(df_raw, header_row, start, end):
    """
    Construye un DataFrame a partir de df_raw usando la fila header_row como columnas,
    y tomando las filas de start+1 .. end-1 como datos.
    """
    header = df_raw.iloc[header_row].fillna("").astype(str).values
    data = df_raw.iloc[header_row+1:end].copy()
    # asignar columnas
    data.columns = header
    # eliminar filas vacías
    data = data.dropna(how="all")
    # resetear index
    data = data.reset_index(drop=True)
    return data


def normalizar_columnas(df):
    cols = (
        pd.Series(df.columns.astype(str))
        .str.upper()
        .str.replace("\n", " ", regex=False)
        .str.replace('"', "", regex=False)
        .str.normalize('NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('utf-8')
        .str.strip()
        .str.replace(" +", " ", regex=True)
    )
    df.columns = cols
    return df


def detectar_columna_fecha(df):
    posibles = [
        "FECHA PROXIMA CALIBRACION",
        "FECHA PROXIMA CALIBRACIÓN",
        "FECHA PROXIMA CALIBRACIÓN",
        "PROXIMA CALIBRACION",
        "PROXIMA CALIBRACIÓN",
        "FECHA PROXIMA CAL"
    ]
    for c in df.columns:
        for p in posibles:
            if p in c:
                return c
    return None


def preparar_bloque(df_block):
    df = df_block.copy()
    df = normalizar_columnas(df)

    col_fecha = detectar_columna_fecha(df)
    if col_fecha is None:
        # No hay fecha en este bloque; lo dejamos, pero marcará sin fecha
        df["FECHA_PROXIMA"] = pd.NaT
    else:
        df.rename(columns={col_fecha: "FECHA_PROXIMA"}, inplace=True)
        df["FECHA_PROXIMA"] = pd.to_datetime(df["FECHA_PROXIMA"], errors="coerce")

    return df


def asignar_tipo_nombre(raw_tipo, indice):
    # normalizar raw_tipo si existe
    if raw_tipo:
        t = raw_tipo.upper().replace(" ", "").replace("-", "")
        if "PLANTA" in t:
            return "PLANTA"
        if "VST2" in t or "VST02" in t:
            return "VST2"
        if "VST3" in t or "VST03" in t:
            return "VST3"
    # fallback por orden
    return ["PLANTA", "VST2", "VST3"][min(indice, 2)]


def filtrar_columnas_para_envio(df):
    # columnas target (normalizadas)
    target = ["IDENTIFICACION", "IDENTIFICACIÓN", "EQUIPO  /  INSTRUMENTO", "EQUIPO / INSTRUMENTO", "FABRICANTE", "FECHA_PROXIMA"]
    # buscar las columnas reales presentes en df
    presentes = []
    cols_upper = [c.upper() for c in df.columns]
    for t in target:
        for c in df.columns:
            if t in c.upper():
                presentes.append(c)
                break
    # eliminar duplicados y mantener orden
    presentes = list(dict.fromkeys(presentes))
    # si FECHA_PROXIMA no está incluida agrégala si existe
    if "FECHA_PROXIMA" in df.columns and "FECHA_PROXIMA" not in presentes:
        presentes.append("FECHA_PROXIMA")
    return df.loc[:, [c for c in presentes if c in df.columns]]


def construir_html_tabla(df, tipo=None):
    if df.empty:
        return "<p>No aplica.</p>"

    df2 = filtrar_columnas_para_envio(df).copy()

    # Mantener el tipo correcto por fila
    if "TIPO" in df.columns:
        df2["TIPO"] = df["TIPO"]
    else:
        df2["TIPO"] = tipo  # fallback si algún bloque no trajo tipo

    # renombrar columnas para mostrarlas limpias
    rename_map = {c: c.title().replace("_", " ") for c in df2.columns}
    df2.rename(columns=rename_map, inplace=True)

    return df2.to_html(index=False, border=1)



def enviar_correo(correo_html):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = "🔔 Alerta de calibraciones – Planta / VST2 / VST3"
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(CORREOS_DESTINO)

    msg.attach(MIMEText(correo_html, "html"))

    server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
    server.starttls()
    server.login(SMTP_USER, SMTP_PASS)
    server.sendmail(SMTP_USER, CORREOS_DESTINO, msg.as_string())
    server.quit()


# ----------------- MAIN -----------------
# ----------------- MAIN CORREGIDO -----------------

def main():
    print("Descargando Excel...")
    bytes_xl = descargar_excel_bytes(EXCEL_URL)

    # Leer hoja completa sin header para detectar posiciones
    df_raw = pd.read_excel(io.BytesIO(bytes_xl), header=None)

    # Encontrar encabezados y bloques
    bloques_meta = encontrar_encabezados_y_bloques(df_raw)

    # Acumuladores globales (ya NO por tipo)
    vencidos_global = []
    proximos_global = []

    for idx, meta in enumerate(bloques_meta):

        blk = construir_df_bloque(df_raw, meta["header_row"], meta["start"], meta["end"])
        blk_preparado = preparar_bloque(blk)

        # Validar si tiene datos reales
        cols_clave = ["IDENTIFICACION", "IDENTIFICACIÓN"]
        tiene_datos = False

        for col in blk_preparado.columns:
            if any(c in col.upper() for c in cols_clave):
                if blk_preparado[col].dropna().astype(str).str.strip().ne("").any():
                    tiene_datos = True
                break

        if not tiene_datos:
            continue

        # Identificar si es PLANTA / VST2 / VST3
        tipo = asignar_tipo_nombre(meta.get("titulo"), idx)
        blk_preparado["TIPO"] = tipo

        # Calcular vencidos y próximos
        hoy = pd.Timestamp("now").normalize()

        if "FECHA_PROXIMA" in blk_preparado.columns:
            blk_preparado["DIAS_RESTANTES"] = (blk_preparado["FECHA_PROXIMA"] - hoy).dt.days

            vencidos = blk_preparado[blk_preparado["DIAS_RESTANTES"] < 0]
            proximos = blk_preparado[
                (blk_preparado["DIAS_RESTANTES"] >= 0)
                & (blk_preparado["DIAS_RESTANTES"] <= DIAS_AVISO)
            ]
        else:
            vencidos = pd.DataFrame()
            proximos = pd.DataFrame()

        # Acumular en listas globales
        if not vencidos.empty:
            vencidos_global.append(vencidos)

        if not proximos.empty:
            proximos_global.append(proximos)

    # Unificar en un solo DataFrame
    vencidos_global = (
        pd.concat(vencidos_global, ignore_index=True)
        if vencidos_global else pd.DataFrame()
    )

    proximos_global = (
        pd.concat(proximos_global, ignore_index=True)
        if proximos_global else pd.DataFrame()
    )

    # Construcción del HTML final (único)
    html_final = f"""
    <h2>🔔 Alerta de calibraciones</h2>
    <p>Instrumentos vencidos y próximos a vencer en {DIAS_AVISO} días.</p>

    <h3>🔴 Vencidos</h3>
    {construir_html_tabla(vencidos_global)}

    <h3>🟠 Próximos a vencer</h3>
    {construir_html_tabla(proximos_global)}
    """

    enviar_correo(html_final)
    print("Correo enviado.")


if __name__ == "__main__":
    main()


