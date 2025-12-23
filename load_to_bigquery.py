import os
from google.cloud import bigquery

# ---------------- CONFIG ----------------
PROJECT_ID = "solar-solution-482101-h0"
DATASET_ID = "employee_dataset"
TABLE_ID = "employee_table"

GCS_URI = "gs://solar-employee-data-ysayaovong/employee_data.csv"

# Masked output as a VIEW (recommended)
MASKED_VIEW_ID = "employee_table_masked"
# ----------------------------------------


def ensure_dataset(client: bigquery.Client) -> None:
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"  # change if your project uses a different location

    try:
        client.get_dataset(dataset_ref)
        print(f"✅ Dataset exists: {PROJECT_ID}.{DATASET_ID}")
    except Exception:
        client.create_dataset(dataset_ref, exists_ok=True)
        print(f"✅ Created dataset: {PROJECT_ID}.{DATASET_ID}")


def load_gcs_csv_to_table(client: bigquery.Client) -> None:
    table_fq = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # overwrite each run
    )

    load_job = client.load_table_from_uri(GCS_URI, table_fq, job_config=job_config)
    load_job.result()

    table = client.get_table(table_fq)
    print(f"✅ Loaded {table.num_rows} rows into {table_fq}")


def create_masked_view(client: bigquery.Client) -> None:
    view_fq = f"{PROJECT_ID}.{DATASET_ID}.{MASKED_VIEW_ID}"
    source_fq = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Masks:
    # - email: keep first 3 chars + domain
    # - phone: replace digits with X
    # - salary: bucketed (or you can null it out)
    # - password: fully masked
    view_sql = f"""
    CREATE OR REPLACE VIEW `{view_fq}` AS
    SELECT
      first_name,
      last_name,
      job_title,
      department,
      CONCAT(SUBSTR(email, 1, 3), "***@", SPLIT(email, "@")[SAFE_OFFSET(1)]) AS email_masked,
      REGEXP_REPLACE(phone_number, r"\\d", "X") AS phone_masked,
      address,
      CASE
        WHEN salary < 50000 THEN "<50k"
        WHEN salary < 80000 THEN "50k-79k"
        WHEN salary < 110000 THEN "80k-109k"
        ELSE "110k+"
      END AS salary_band,
      "********" AS password_masked
    FROM `{source_fq}`;
    """

    client.query(view_sql).result()
    print(f"✅ Created/updated masked view: {view_fq}")


def main():
    # Uses GOOGLE_APPLICATION_CREDENTIALS env var you already set in PowerShell
    client = bigquery.Client(project=PROJECT_ID)

    ensure_dataset(client)
    load_gcs_csv_to_table(client)
    create_masked_view(client)

    print("\nDone.")
    print(f"Raw table:    {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    print(f"Masked view:  {PROJECT_ID}.{DATASET_ID}.{MASKED_VIEW_ID}")


if __name__ == "__main__":
    main()
