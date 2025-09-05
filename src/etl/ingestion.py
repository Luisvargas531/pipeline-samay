import os, json, re
from pathlib import Path
import pandas as pd

from src.etl.utils import (
    empty_to_none, norm_gender, parse_date_any, parse_sample_rate, parse_from_filename,
    stable_recording_id, infer_pid_from_path, probe_wav
)

TABULAR_EXTS = {".csv", ".tsv", ".txt", ".xlsx", ".json", ".jsonl"}

def safe_read_table(p: Path) -> pd.DataFrame:
    try:
        suf = p.suffix.lower()
        if suf == ".csv":
            return pd.read_csv(p, encoding="utf-8", on_bad_lines="skip")
        if suf == ".tsv":
            return pd.read_csv(p, sep="\t", encoding="utf-8", on_bad_lines="skip")
        if suf == ".txt":
            try:
                return pd.read_csv(p, encoding="utf-8", on_bad_lines="skip")
            except Exception:
                try:
                    return pd.read_csv(p, delim_whitespace=True, encoding="utf-8", on_bad_lines="skip")
                except Exception:
                    return pd.DataFrame({"raw_text": Path(p).read_text(encoding="utf-8", errors="ignore").splitlines()})
        if suf == ".xlsx":
            frames = []
            try:
                xls = pd.ExcelFile(p)
                for sh in xls.sheet_names:
                    try:
                        df = pd.read_excel(p, sheet_name=sh)
                        df["__sheet__"] = sh
                        frames.append(df)
                    except Exception:
                        pass
            except Exception:
                pass
            return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if suf == ".jsonl":
            rows = []
            with p.open("r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rows.append(json.loads(line))
                    except Exception:
                        rows.append({"__raw__": line})
            return pd.DataFrame(rows)
        if suf == ".json":
            try:
                obj = json.loads(p.read_text(encoding="utf-8", errors="ignore"))
                if isinstance(obj, list):
                    return pd.DataFrame(obj)
                if isinstance(obj, dict):
                    try:
                        return pd.json_normalize(obj)
                    except Exception:
                        return pd.DataFrame([obj])
            except Exception:
                return pd.DataFrame()
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

def ingest_all(input_dir: Path) -> pd.DataFrame:
    records = []

    # ---------- PASO 1: fuentes tabulares ----------
    for r, d, fs in os.walk(input_dir):
        for f in fs:
            p = Path(r) / f
            if p.suffix.lower() in TABULAR_EXTS:
                df = safe_read_table(p)
                if df.empty:
                    continue
                colmap = {c.lower(): c for c in df.columns}

                def pick(*names):
                    for nm in names:
                        if nm in colmap:
                            return colmap[nm]
                    return None

                c_age   = pick("age","edad")
                c_dur   = pick("duration","duration_s","length","seconds","duracion","duración")
                c_gen   = pick("gender","sex","genero","género")
                c_pid   = pick("patient_id","patient","pid","id_paciente","paciente")
                c_dx    = pick("diagnosis","label","condition","diagnostico","diagnóstico")
                c_fn    = pick("filename","file","wav","name","archivo")
                c_loc   = pick("location","body_location","auscultation_site","site","ubicacion","ubicación")
                c_date  = pick("recording_date","date","datetime","timestamp","fecha")
                c_sr    = pick("sample_rate","samplerate","fs","sample_rate_hz")
                c_bit   = pick("bit_depth","bits")
                c_filter= pick("filter_mode","filter")
                c_hosp  = pick("site","hospital","clinic","center","centro","sede")

                for _, row in df.iterrows():
                    fn = empty_to_none(row.get(c_fn)) if c_fn else None
                    if fn:
                        fn = Path(str(fn)).name  # normalizar a basename
                    pid = empty_to_none(row.get(c_pid)) if c_pid else None
                    loc = empty_to_none(row.get(c_loc)) if c_loc else None
                    dtx = row.get(c_date) if c_date else None

                    if (pid is None) and fn:
                        fpid, floc, fdate, ffilter = parse_from_filename(fn)
                        pid = pid or fpid
                        loc = loc or floc
                        dtx = dtx or fdate
                        filter_mode = ffilter or empty_to_none(row.get(c_filter)) if c_filter else ffilter
                    else:
                        filter_mode = empty_to_none(row.get(c_filter)) if c_filter else None

                    ts = parse_date_any(dtx) if dtx is not None else pd.NaT
                    # Fallback de timestamp: mtime del archivo tabular
                    if pd.isna(ts):
                        try:
                            ts = pd.to_datetime(Path(p).stat().st_mtime, unit='s')
                        except Exception:
                            pass

                    age = float(row[c_age]) if c_age and pd.notna(row.get(c_age)) else None
                    sr  = parse_sample_rate(row.get(c_sr)) if c_sr else None
                    duration = float(row[c_dur]) if c_dur and pd.notna(row.get(c_dur)) else None

                    try:
                        rel = Path(r).relative_to(input_dir)
                        source_name = rel.parts[0] if len(rel.parts)>0 else "root"
                    except Exception:
                        source_name = "root"
                    hospital_site = empty_to_none(row.get(c_hosp)) if c_hosp and pd.notna(row.get(c_hosp)) else source_name

                    # Fallback extra: patient_id desde la RUTA
                    if pid is None:
                        pid = infer_pid_from_path(p)

                    rec = {
                        "recording_id": None,
                        "patient_id": empty_to_none(pid),
                        "timestamp": ts,
                        "duration_seconds": duration,
                        "sample_rate": sr,
                        "bit_depth": int(row[c_bit]) if c_bit and pd.notna(row.get(c_bit)) else None,
                        "filter_mode": empty_to_none(filter_mode),
                        "recording_location": empty_to_none(loc),
                        "file_path": empty_to_none(fn),
                        "diagnosis": empty_to_none(row.get(c_dx)) if c_dx else None,
                        "age": age,
                        "gender": norm_gender(row.get(c_gen)) if c_gen else None,
                        "hospital_site": empty_to_none(hospital_site),
                        "source_name": source_name,
                        "__origin__": str(p)
                    }
                    rec["recording_id"] = stable_recording_id(rec["source_name"], rec["file_path"], str(rec["timestamp"]))
                    records.append(rec)

    # Índice por basename de file_path para enriquecer con WAV
    def basename(x):
        return Path(str(x)).name if x else None
    idx_by_base = {}
    for i, rec in enumerate(records):
        b = basename(rec.get("file_path"))
        if b:
            idx_by_base.setdefault(b, []).append(i)

    # ---------- PASO 2: escanear WAV y ENRIQUECER/CREAR ----------
    for r, d, fs in os.walk(input_dir):
        for f in fs:
            p = Path(r) / f
            if p.suffix.lower() != ".wav":
                continue

            base = p.name
            meta = probe_wav(p)  # {sample_rate,duration_seconds,bit_depth,channels,timestamp}
            fpid, floc, fdate, ffilter = parse_from_filename(base)

            if base in idx_by_base:
                # Enriquecer filas existentes con ese basename
                for i in idx_by_base[base]:
                    rec = records[i]
                    if not rec.get("sample_rate") or pd.isna(rec.get("sample_rate")):
                        if meta.get("sample_rate") is not None:
                            rec["sample_rate"] = meta["sample_rate"]
                    if not rec.get("duration_seconds") or pd.isna(rec.get("duration_seconds")):
                        if meta.get("duration_seconds") is not None:
                            rec["duration_seconds"] = meta["duration_seconds"]
                    if not rec.get("bit_depth") or pd.isna(rec.get("bit_depth")):
                        if meta.get("bit_depth") is not None:
                            rec["bit_depth"] = meta["bit_depth"]
                    if not rec.get("timestamp") or pd.isna(rec.get("timestamp")):
                        rec["timestamp"] = meta.get("timestamp") or fdate
                    if (not rec.get("patient_id")) and fpid:
                        rec["patient_id"] = fpid
                    if (not rec.get("recording_location")) and floc:
                        rec["recording_location"] = floc
                    if (not rec.get("filter_mode")) and ffilter:
                        rec["filter_mode"] = ffilter
                continue  # no crear fila nueva

            # Si no estaba listado, crear nueva fila desde el WAV
            try:
                rel = p.relative_to(input_dir)
                source_name = rel.parts[0] if len(rel.parts)>0 else "root"
            except Exception:
                source_name = "root"
            hospital_site = source_name

            rec = {
                "recording_id": None,
                "patient_id": empty_to_none(fpid),
                "timestamp": meta.get("timestamp") or fdate,
                "duration_seconds": meta.get("duration_seconds"),
                "sample_rate": meta.get("sample_rate"),
                "bit_depth": meta.get("bit_depth"),
                "filter_mode": empty_to_none(ffilter),
                "recording_location": empty_to_none(floc),
                "file_path": base,  # basename
                "diagnosis": None,
                "age": None,
                "gender": None,
                "hospital_site": hospital_site,
                "source_name": source_name,
                "__origin__": str(p)
            }
            rec["recording_id"] = stable_recording_id(rec["source_name"], rec["file_path"], str(rec["timestamp"]))
            records.append(rec)
            idx_by_base.setdefault(base, []).append(len(records)-1)

    df = pd.DataFrame(records)

    # ---------- PASO 3: limpiar filas vacías / normalizar blancos ----------
    if not df.empty:
        # estandarizar blancos a NaN
        for c in ["file_path","patient_id","recording_location","filter_mode","diagnosis","hospital_site","source_name"]:
            if c in df.columns:
                df[c] = df[c].apply(empty_to_none)
        # descartar filas sin file_path y sin sample_rate y sin timestamp
        mask_empty = df["file_path"].isna() & df["sample_rate"].isna() & df["timestamp"].isna()
        df = df.loc[~mask_empty].copy()

        # ---------- PASO 4: deduplicar por (source_name,file_path) quedando con la fila más completa ----------
        if "file_path" in df.columns and "source_name" in df.columns:
            df["__non_null__"] = df.notna().sum(axis=1)
            df = (df
                  .sort_values(["source_name","file_path","__non_null__"], ascending=[True, True, False])
                  .drop_duplicates(subset=["source_name","file_path"], keep="first")
                  .drop(columns="__non_null__"))

    return df


