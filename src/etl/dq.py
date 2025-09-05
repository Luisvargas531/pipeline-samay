import pandas as pd
import numpy as np

# Campos críticos que se reportan como "error" si faltan
REQUIRED_ERROR = ["file_path", "sample_rate"]
# Campos importantes pero tolerables -> "warning"
REQUIRED_WARNING = ["patient_id", "timestamp"]

class DataQualityValidator:
    def validate_audio_file(self, file_path: str):
        issues = []
        if not file_path or str(file_path).strip() == "":
            issues.append(("missing_field", "file_path", "error"))
        else:
            ext = str(file_path).lower().rsplit(".", 1)[-1]
            if ext not in ("wav", "flac", "mp3"):
                # extensión inesperada no bloquea: warning
                issues.append(("unexpected_extension", ext, "warning"))
        return issues

    def validate_metadata(self, row: pd.Series):
        issues = []

        # 1) Faltantes "error" (críticos)
        for c in REQUIRED_ERROR:
            v = row.get(c, None)
            if pd.isna(v) or (c == "file_path" and (v is None or str(v).strip() == "")):
                issues.append(("missing_field", c, "error"))

        # 2) Faltantes "warning" (tolerables)
        for c in REQUIRED_WARNING:
            v = row.get(c, None)
            if pd.isna(v) or (isinstance(v, str) and v.strip() == ""):
                issues.append(("missing_field", c, "warning"))

        # 3) Rango/valores
        age = row.get("age", np.nan)
        if pd.notna(age) and (age < 0 or age > 120):
            issues.append(("invalid_age", age, "warning"))

        dur = row.get("duration_seconds", np.nan)
        if pd.notna(dur):
            try:
                if not (2 <= float(dur) <= 60):
                    issues.append(("duration_out_of_range", dur, "warning"))
            except Exception:
                issues.append(("duration_out_of_range", dur, "warning"))

        # sample_rate inválido (SOLO si no está faltante)
        sr = row.get("sample_rate", np.nan)
        if pd.isna(sr):
            # ya se reportó como missing_field (error) arriba
            pass
        else:
            try:
                if int(sr) <= 0:
                    issues.append(("invalid_sample_rate", sr, "error"))
            except Exception:
                issues.append(("invalid_sample_rate", sr, "error"))

        # Género faltante: informativo
        if pd.isna(row.get("gender", np.nan)):
            issues.append(("missing_gender", "gender null or unrecognized", "note"))

        return issues

def run_dq(audio_df: pd.DataFrame) -> pd.DataFrame:
    if audio_df is None or audio_df.empty:
        return pd.DataFrame(columns=["recording_id","issue_type","detail","severity"])

    v = DataQualityValidator()
    issues = []

    # Validación fila a fila
    for _, r in audio_df.iterrows():
        rid = r.get("recording_id")
        for k, d, sev in v.validate_metadata(r):
            issues.append({"recording_id": rid, "issue_type": k, "detail": d, "severity": sev})
        for k, d, sev in v.validate_audio_file(r.get("file_path")):
            issues.append({"recording_id": rid, "issue_type": k, "detail": d, "severity": sev})

    # Duplicados: sólo si hay clave fuerte -> warning
    tmp = audio_df.copy()
    tmp["ts_date"] = pd.to_datetime(tmp["timestamp"], errors="coerce").dt.date.astype(str)
    ok = tmp["file_path"].notna() & tmp["source_name"].notna() & (tmp["patient_id"].notna() | tmp["timestamp"].notna())
    if ok.any():
        sub = tmp.loc[ok, ["source_name", "file_path", "patient_id", "ts_date"]].astype(str)
        key = sub.apply(
            lambda r: (r["source_name"], r["file_path"], r["patient_id"] if r["patient_id"] not in ["", "nan", "None"] else r["ts_date"]),
            axis=1
        ).astype(str)
        dup_mask = key.duplicated(keep=False)
        dup_ids = tmp.loc[ok].loc[dup_mask, "recording_id"].tolist()
        for rid in dup_ids:
            issues.append({
                "recording_id": rid,
                "issue_type": "possible_duplicate",
                "detail": "same (source,file + patient_id|date)",
                "severity": "warning"
            })

    return pd.DataFrame(issues)
