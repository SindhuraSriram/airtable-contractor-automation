import os
import json
from pyairtable import Api
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("BASE_ID")

# Table names
APPLICANTS = "Applicants"
PERSONAL = "Personal Details"
EXPERIENCE = "Work Experience"
SALARY = "Salary Preferences"

# Initialize API and tables
api = Api(API_KEY)
applicants_tbl = api.table(BASE_ID, APPLICANTS)
personal_tbl = api.table(BASE_ID, PERSONAL)
experience_tbl = api.table(BASE_ID, EXPERIENCE)
salary_tbl = api.table(BASE_ID, SALARY)

def clear_old_records(table, applicant_id):
    try:
        records = table.all(formula=f"{{Applicant}} = '{applicant_id}'")
        for rec in records:
            table.delete(rec["id"])
    except Exception as e:
        print(f"❌ Failed to clear records in {table.name} for applicant {applicant_id}: {e}")

def decompress_applicant(applicant):
    applicant_id = applicant.get("id")
    fields = applicant.get("fields", {})
    compressed = fields.get("Compressed JSON")

    if not applicant_id or not compressed:
        print(f"⚠️ Skipping {applicant_id}: Missing ID or Compressed JSON.")
        return

    try:
        data = json.loads(compressed)
    except json.JSONDecodeError:
        print(f"❌ Invalid JSON format for applicant {applicant_id}")
        return

    try:
        # Optional: Clear previous linked records
        clear_old_records(personal_tbl, applicant_id)
        clear_old_records(experience_tbl, applicant_id)
        clear_old_records(salary_tbl, applicant_id)

        # Personal (1:1)
        personal = data.get("personal", {})
        if personal:
            personal_tbl.create({
                "Full Name": personal.get("name"),
                "Location": personal.get("location"),
                "Applicant": [applicant_id]
            })

        # Experience (1:N)
        for exp in data.get("experience", []):
            if not isinstance(exp, dict):
                continue
            experience_tbl.create({
                "Company": exp.get("company"),
                "Title": exp.get("title"),
                "Applicant": [applicant_id]
            })

        # Salary (1:1)
        salary = data.get("salary", {})
        if salary:
            salary_tbl.create({
                "Preferred Rate": salary.get("rate"),
                "Currency": salary.get("currency"),
                "Availability": salary.get("availability"),
                "Applicant": [applicant_id]
            })

        print(f"✅ Decompressed and synced for applicant {applicant_id}")

    except Exception as e:
        print(f"❌ Error decompressing applicant {applicant_id}: {e}")

def main():
    try:
        applicants = applicants_tbl.all()
        for applicant in applicants:
            decompress_applicant(applicant)
    except Exception as e:
        print(f"❌ Fatal error loading applicants: {e}")

if __name__ == "__main__":
    main()
