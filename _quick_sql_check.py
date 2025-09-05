import sqlite3, pandas as pd

con = sqlite3.connect(r".\cleaned_data\samay_lung.db")
queries = [
    "SELECT COUNT(*) AS n_audio FROM audio_recordings",
    "SELECT COUNT(DISTINCT patient_id) AS n_patients FROM audio_recordings WHERE patient_id IS NOT NULL",
    "SELECT issue_type, severity, COUNT(*) c FROM data_quality_issues GROUP BY 1,2 ORDER BY c DESC LIMIT 10",
]
for q in queries:
    print("\nSQL>", q)
    print(pd.read_sql(q, con))
con.close()
