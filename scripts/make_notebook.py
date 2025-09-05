import nbformat as nbf

nb = nbf.v4.new_notebook()
cells = []

cells.append(nbf.v4.new_markdown_cell("# Samay Lung Sound – ETL & Data Quality Report\n\n**Autor:** _<Tu Nombre>_\n\nEste notebook acompaña la entrega del pipeline. Lee las salidas de `./cleaned_data/`, resume calidad y corre consultas SQL."))

cells.append(nbf.v4.new_markdown_cell("## 0. Quickstart (reproducir)\n```bash\npython -m venv .venv\n# Windows: .\\.venv\\Scripts\\Activate.ps1\npip install -r requirements.txt\npython -m src.cli --input \"./raw_ingest\" --output \"./cleaned_data\"\n```"))

cells.append(nbf.v4.new_markdown_cell("## 1. Cargar salidas"))
cells.append(nbf.v4.new_code_cell("import pandas as pd\nfrom pathlib import Path\nbase = Path('./cleaned_data')\naudio = pd.read_csv(base/'audio_recordings.csv')\npatients = pd.read_csv(base/'patient_demographics.csv')\ndq = pd.read_csv(base/'data_quality_report.csv')\naudio.head(3), patients.head(3), dq.head(3)"))

cells.append(nbf.v4.new_markdown_cell("## 2. Métricas básicas"))
cells.append(nbf.v4.new_code_cell("print('Registros de audio:', len(audio))\nprint('Pacientes (no nulos):', audio['patient_id'].dropna().nunique())\nprint('\\nTop issues:')\nprint(dq.groupby(['severity','issue_type']).size().reset_index(name='count').sort_values(['severity','count'], ascending=[True, False]).head(10).to_string(index=False))"))

cells.append(nbf.v4.new_markdown_cell("## 3. Visualizaciones"))
cells.append(nbf.v4.new_code_cell("import matplotlib.pyplot as plt\nsr_counts = (audio['sample_rate'].dropna().astype(int).value_counts().head(10))\nplt.figure(); sr_counts.plot(kind='bar'); plt.title('Top 10 sample_rate (Hz)'); plt.xlabel('sample_rate'); plt.ylabel('count'); plt.tight_layout(); plt.show()"))
cells.append(nbf.v4.new_code_cell("dur = audio['duration_seconds'].dropna().astype(float)\ndur_clip = dur[(dur>=0) & (dur<=120)]\nplt.figure(); plt.hist(dur_clip, bins=20); plt.title('Distribución de duración (s)'); plt.xlabel('seconds'); plt.ylabel('count'); plt.tight_layout(); plt.show()"))

cells.append(nbf.v4.new_markdown_cell("## 4. Consultas a SQLite"))
cells.append(nbf.v4.new_code_cell("import sqlite3\ncon = sqlite3.connect('./cleaned_data/samay_lung.db')\nqs = [\n  \"SELECT COUNT(*) AS n_audio FROM audio_recordings\",\n  \"SELECT COUNT(DISTINCT patient_id) AS n_patients FROM audio_recordings WHERE patient_id IS NOT NULL\",\n  \"SELECT issue_type, severity, COUNT(*) c FROM data_quality_issues GROUP BY 1,2 ORDER BY c DESC LIMIT 10\",\n]\nimport pandas as pd\nfor q in qs:\n    print('\\nSQL>', q)\n    print(pd.read_sql(q, con))\ncon.close()"))

nb['cells'] = cells
out = 'Samay_Lung_Pipeline_Report.ipynb'
with open(out, 'w', encoding='utf-8') as f: nbf.write(nb, f)
print('[OK] Creado:', out)
