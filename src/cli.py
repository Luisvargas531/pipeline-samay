import argparse
from pathlib import Path
from src.etl.ingestion import ingest_all
from src.etl.normalize import split_tables
from src.etl.dq import run_dq
from src.etl.load import save_outputs, write_partitions

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=str, default="./raw_ingest")
    ap.add_argument("--output", type=str, default="./cleaned_data")
    ap.add_argument("--partitions", action="store_true", help="Write date-partitioned outputs")
    args = ap.parse_args()

    input_dir = Path(args.input)
    output_dir = Path(args.output)

    if not input_dir.exists():
        print(f"[ERROR] Input no existe: {input_dir.resolve()}")
        return

    print(f"[START] Ingestando desde: {input_dir.resolve()}")
    staging = ingest_all(input_dir)
    print(f"[INGEST] Registros en staging: {len(staging)}")

    audio, patients = split_tables(staging)
    print(f"[NORM] audio={len(audio)}  patients={len(patients)}")

    dq = run_dq(staging)
    print(f"[DQ] issues={len(dq)}")

    save_outputs(audio, patients, dq, output_dir)
    print(f"[OUT] Escribí salidas en: {output_dir.resolve()}")

    if args.partitions:
        write_partitions(audio, output_dir / "partitions")
        print(f"[OUT] Particiones en: {(output_dir / 'partitions').resolve()}")

    print("[DONE] Pipeline OK")

if __name__ == "__main__":
    main()
