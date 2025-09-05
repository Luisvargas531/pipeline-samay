import pandas as pd

def split_tables(df: pd.DataFrame):
    audio_cols = [
        "recording_id","patient_id","timestamp","duration_seconds","sample_rate","bit_depth",
        "filter_mode","recording_location","file_path","diagnosis","hospital_site","source_name"
    ]
    patients_cols = ["patient_id","age","gender","hospital_site"]

    audio = df[audio_cols].copy() if not df.empty else pd.DataFrame(columns=audio_cols)
    patients = (
        df.dropna(subset=["patient_id"])
          .sort_values("timestamp")
          .groupby("patient_id", as_index=False)
          .agg({
              "age":"max",
              "gender": lambda s: s.dropna().iloc[0] if s.dropna().size else None,
              "hospital_site": lambda s: s.dropna().iloc[0] if s.dropna().size else None
          })
    )
    patients = patients[patients_cols]
    return audio, patients




