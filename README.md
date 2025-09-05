# Samay Lung Sound – Data Engineering Pipeline

Pipeline para **procesar, organizar y almacenar** grabaciones de sonidos pulmonares y su metadata heterogénea (CSV/JSON/XLSX/TXT y, si existen, WAV). Entrega datos estandarizados, **reporte de calidad con severidades** y scripts SQL listos para producción.

---

## 🚀 Quickstart

> Requiere Python 3.10+.

```bash
# Crear entorno e instalar dependencias
python -m venv .venv
# macOS/Linux:
source .venv/bin/activate
# Windows PowerShell:
# .\.venv\Scripts\Activate.ps1

pip install -U pip
pip install -r requirements.txt

# Ejecutar el pipeline (ajusta --input si tu ruta es distinta)
python -m src.cli --input "./raw_ingest" --output "./cleaned_data"

# Particionado por fecha (opcional)
python -m src.cli --input "./raw_ingest" --output "./cleaned_data" --partitions
