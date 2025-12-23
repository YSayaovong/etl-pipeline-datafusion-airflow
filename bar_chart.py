from google.cloud import bigquery
import matplotlib.pyplot as plt

PROJECT_ID = "solar-solution-482101-h0"
VIEW_FQ = f"{PROJECT_ID}.employee_dataset.employee_table_masked"

QUERY = f"""
SELECT department, COUNT(*) AS employee_count
FROM `{VIEW_FQ}`
GROUP BY department
ORDER BY employee_count DESC
LIMIT 10
"""

client = bigquery.Client(project=PROJECT_ID)
rows = client.query(QUERY).result()

departments = []
counts = []

for r in rows:
    departments.append(r["department"])
    counts.append(r["employee_count"])

plt.figure()
plt.bar(departments, counts)
plt.xticks(rotation=45, ha="right")
plt.xlabel("Department")
plt.ylabel("Employees")
plt.title("Employees by Department (Top 10)")
plt.tight_layout()
plt.savefig("employees_by_department.png", dpi=200)

print("Saved: employees_by_department.png")
