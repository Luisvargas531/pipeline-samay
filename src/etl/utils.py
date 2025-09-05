import re
import hashlib
import pandas as pd
import numpy as np

# ---------------------------
# Normalizadores y parsers
# ---------------------------

def norm_gender(x):
    if pd.isna(x):
        return np.nan
    s = str(x).strip().lower()
    if s in ["m","male","masculino","h","man","masc"]:
        return "M"
    if s in ["f","female","femenino","w","woman","fem"]:
        return "F"
    return np.nan

def parse_date_any(x):
    if pd.isna(x):
        return pd.NaT
    s = str(x).strip()
    if not s:
        return pd.NaT
    fmts = [
        "%Y-%m-%d","%Y%m%d","%d/%m/%Y","%m/%d/%Y",
        "%d-%m-%Y","%m-%d-%Y","%Y/%m/%d","%d.%m.%Y"
    ]
    for f in fmts:
        try:
            return pd.to_datetime(s, format=f, errors="raise")
        except Exception:
            pass
    return pd.to_datetime(s, errors="coerce")

def parse_sample_rate(x):
    """
    Acepta: 48000, '48k', '48kHz', '44.1kHz', '44100Hz', etc.
    """
    import math
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return np.nan
    s = str(x).strip().lower()

    m = re.match(r'^\s*([0-9]+(?:\.[0-9]+)?)\s*k(?:hz)?\s*$', s)
    if m:
        try:
            return int(float(m.group(1)) * 1000)
        except Exception:
            return np.nan

    s2 = s.replace("khz", "000").replace("k", "000").replace("hz", "")
    s2 = re.sub(r"[^0-9]", "", s2)
    if not s2:
        return np.nan
    try:
        return int(s2)
    except Exception:
        return np.nan

def parse_from_filename(fn):
    """
    Extrae patient_id, ubicación, fecha (YYYYMMDD), filtro desde el nombre del archivo.
    Soporta P12345, ID12345, PAT0001, o 6+ dígitos con 0–2 letras prefijo.
    """
    pid = None; loc = None; dt = None; filt = None
    base = str(fn).split("/")[-1]

    for pat in [
        r"(?:^|[_-])(P\d{3,6})(?:[_-]|$)",
        r"(?:^|[_-])(ID\d{3,6})(?:[_-]|$)",
        r"(?:^|[_-])(PAT\d{3,6})(?:[_-]|$)",
        r"(?:^|[_-])([A-Za-z]{0,2}\d{6,})(?:[_-]|$)"
    ]:
        m = re.search(pat, base, flags=re.I)
        if m:
            pid = m.group(1).upper()
            break

    m2 = re.search(r"(Anterior|Posterior)[ _-]?(Left|Right)?[ _-]?(Upper|Middle|Lower)?", base, flags=re.I)
    if m2:
        parts = [w for w in m2.groups() if w]
        loc = "_".join([w.capitalize() for w in parts]) if parts else None

    m3 = re.search(r"(\d{8})", base)
    if m3:
        try:
            dt = pd.to_datetime(m3.group(1), format="%Y%m%d", errors="coerce")
        except Exception:
            pass

    m4 = re.search(r"(Bell|Diaphragm|Extended)", base, flags=re.I)
    if m4:
        filt = m4.group(1).capitalize()

    return pid, loc, dt, filt

def stable_recording_id(source, filename, ts):
    s = f"{source}|{filename}|{ts}"
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def infer_pid_from_path(path_like):
    """Intenta extraer patient_id desde la ruta (carpetas/nombres)."""
    s = str(path_like)
    m = re.search(r'(?:^|[\\/._-])(P\d{3,6}|ID\d{3,6}|PAT\d{3,6}|[A-Za-z]{0,2}\d{6,})(?:[\\/._-]|$)', s, flags=re.I)
    return m.group(1).upper() if m else None

# ---------------------------
# Lectura de WAV (metadatos)
# ---------------------------

def probe_wav(file_path):
    """
    Devuelve metadatos del WAV:
    - sample_rate, duration_seconds, bit_depth, channels, timestamp
    Con fallback por tamaño si getnframes()==0.
    """
    import wave, contextlib, os
    try:
        with contextlib.closing(wave.open(str(file_path), 'rb')) as w:
            sr = w.getframerate()
            nframes = w.getnframes()
            sampwidth = w.getsampwidth()  # bytes por muestra
            channels = w.getnchannels()
            if sr and nframes:
                duration = nframes / float(sr)
            else:
                size = os.path.getsize(file_path)
                data_bytes = max(size - 44, 0)
                bpf = max(sampwidth * channels, 1)
                frames = data_bytes / bpf
                duration = (frames / sr) if sr else None
            bit_depth = (sampwidth * 8) if sampwidth else None
        ts = pd.to_datetime(os.path.getmtime(file_path), unit='s')
        return {"sample_rate": sr, "duration_seconds": duration, "bit_depth": bit_depth, "channels": channels, "timestamp": ts}
    except Exception:
        return {}
def empty_to_none(x):
    if x is None:
        return None
    try:
        import pandas as pd
        if isinstance(x, float) and pd.isna(x):
            return None
    except Exception:
        pass
    s = str(x).strip()
    return None if s=="" or s.lower() in ("nan","none","null") else s
