import os
import csv
import random
import string
from faker import Faker
from google.cloud import storage

# ---------- CONFIG ----------
PROJECT_ID = "solar-solution-482101-h0"
BUCKET_NAME = "solar-employee-data-ysayaovong"
LOCAL_CSV = "employee_data.csv"
NUM_EMPLOYEES = 100

# If you want to rely on GOOGLE_APPLICATION_CREDENTIALS env var, leave this as None.
# Otherwise, hardcode your service account key path here:
SERVICE_ACCOUNT_KEY_PATH = None  # e.g. r"E:\...\service_account.json"
# ----------------------------

# (Optional) Force credentials from a specific JSON key file
if SERVICE_ACCOUNT_KEY_PATH:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY_PATH

# Create Faker instance
fake = Faker()

# Define the character set for the password
password_characters = string.ascii_letters + string.digits

# Generate employee data and save to a CSV file
with open(LOCAL_CSV, mode="w", newline="", encoding="utf-8") as file:
    fieldnames = [
        "first_name",
        "last_name",
        "job_title",
        "department",
        "email",
        "address",
        "phone_number",
        "salary",
        "password",
    ]
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()

    for _ in range(NUM_EMPLOYEES):
        writer.writerow(
            {
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "job_title": fake.job(),
                "department": fake.job(),  # simple placeholder
                "email": fake.email(),
                "address": fake.city(),
                "phone_number": fake.phone_number(),
                "salary": random.randint(30000, 120000),
                "password": "".join(random.choice(password_characters) for _ in range(10)),
            }
        )

print(f"✅ Generated {LOCAL_CSV} with {NUM_EMPLOYEES} employees.")

# Upload the CSV to Google Cloud Storage
storage_client = storage.Client(project=PROJECT_ID)
bucket = storage_client.bucket(BUCKET_NAME)

blob = bucket.blob(LOCAL_CSV)  # object name in GCS
blob.upload_from_filename(LOCAL_CSV)

print(f"✅ Uploaded {LOCAL_CSV} to gs://{BUCKET_NAME}/{LOCAL_CSV}")
