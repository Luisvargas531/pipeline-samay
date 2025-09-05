from pathlib import Path
import pandas as pd
import sqlite3

def save_outputs(audio_df: pd.DataFrame, patients_df: pd.DataFrame, dq_df: pd.DataFrame, outdir: Path):
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "audio_recordings.csv").write_text(audio_df.to_csv(index=False), encoding="utf-8")
    (outdir / "patient_demographics.csv").write_text(patients_df.to_csv(index=False), encoding="utf-8")
    (outdir / "data_quality_report.csv").write_text(dq_df.to_csv(index=False), encoding="utf-8")

    db_path = outdir / "samay_lung.db"
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(db_path)
    audio_df.to_sql("audio_recordings", conn, index=False)
    patients_df.to_sql("patient_demographics", conn, index=False)
    dq_df.to_sql("data_quality_issues", conn, index=False)
    conn.close()

def write_partitions(audio_df: pd.DataFrame, outdir: Path):
    audio_df = audio_df.copy()
    audio_df["date"] = pd.to_datetime(audio_df["timestamp"]).dt.date.astype(str)
    for (d), chunk in audio_df.groupby(["date"]):
        p = outdir / f"date={d}"
        p.mkdir(parents=True, exist_ok=True)
        (p / "audio_recordings.csv").write_text(chunk.drop(columns=["date"]).to_csv(index=False), encoding="utf-8")
