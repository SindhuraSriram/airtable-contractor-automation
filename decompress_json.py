import os
import json
from pyairtable import Api
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("BASE_ID")

# Airtable table names
APPLICANTS = "Applicants"
PERSONAL = "Personal Details"
EXPERIENCE = "Work Experience"
SALARY = "Salary Preferences"

api = Api(API_KEY)

# Table handles
applicants_tbl = api.table(BASE_ID, APPLICANTS)
personal_tbl = api.table(BASE_ID, PERSONAL)
experience_tbl = api.table(BASE_ID, EXPERIENCE)
salary_tbl = api.table(BASE_ID, SALARY)

def clear_old_records(table, applicant_id):
    """Delete child records linked to the given applicant."""
    records = table.all(formula=f"{{Applicant}} = '{applicant_id}'")
    for rec in records:
        table.delete(rec['id'])

def decompress_applicant(applicant):
    applicant_id = applicant['id']
    fields = applicant['fields']
    compressed_json = fields.get("Compressed JSON")

    if not compressed_json:
        print(f"⏭️ No compressed JSON for Applicant {applicant_id}")
        return

    try:
        data = json.loads(compressed_json)
    except json.JSONDecodeError:
        print(f"❌ Invalid JSON for Applicant {applicant_id}")
        return

    # Optional: Clear existing child records (overwrite mode)
    clear_old_records(personal_tbl, applicant_id)
    clear_old_records(experience_tbl, applicant_id)
    clear_old_records(salary_tbl, applicant_id)

    # Insert Personal Details
    personal = data.get("personal", {})
    if personal:
        personal_tbl.create({
            "Full Name": personal.get("name"),
            "Location": personal.get("location"),
            "Applicant": [applicant_id]
        })

    # Insert Work Experience (multiple)
    for exp in data.get("experience", []):
        experience_tbl.create({
            "Company": exp.get("company"),
            "Title": exp.get("title"),
            "Applicant": [applicant_id]
        })

    # Insert Salary Preferences
    salary = data.get("salary", {})
    if salary:
        salary_tbl.create({
            "Preferred Rate": salary.get("rate"),
            "Currency": salary.get("currency"),
            "Availability": salary.get("availability"),
            "Applicant": [applicant_id]
        })

    print(f"✅ Decompressed and synced for Applicant {applicant_id}")

def main():
    applicants = applicants_tbl.all()
    for applicant in applicants:
        decompress_applicant(applicant)

if __name__ == "__main__":
    main()
