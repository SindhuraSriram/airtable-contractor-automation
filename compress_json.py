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

# Initialize all tables
api = Api(API_KEY)

applicants_tbl = api.table(BASE_ID, APPLICANTS)
personal_tbl = api.table(BASE_ID, PERSONAL)
experience_tbl = api.table(BASE_ID, EXPERIENCE)
salary_tbl = api.table(BASE_ID, SALARY)

def get_linked_records(table, applicant_id):
    """Fetch records from a child table linked to a specific Applicant ID."""
    records = table.all(formula=f"{{Applicant}} = '{applicant_id}'")
    return records

def compress_applicant(applicant_record):
    applicant_id = applicant_record['id']

    # Personal Details (1:1)
    personal = get_linked_records(personal_tbl, applicant_id)
    personal_data = personal[0]['fields'] if personal else {}

    # Work Experience (1:N)
    experience = get_linked_records(experience_tbl, applicant_id)
    experience_data = [
        {
            "company": exp['fields'].get("Company"),
            "title": exp['fields'].get("Title")
        }
        for exp in experience
    ]

    # Salary Preferences (1:1)
    salary = get_linked_records(salary_tbl, applicant_id)
    salary_data = salary[0]['fields'] if salary else {}

    # Build compressed JSON
    compressed = {
        "personal": {
            "name": personal_data.get("Full Name"),
            "location": personal_data.get("Location")
        },
        "experience": experience_data,
        "salary": {
            "rate": salary_data.get("Preferred Rate"),
            "currency": salary_data.get("Currency"),
            "availability": salary_data.get("Availability")
        }
    }

    # Write to Applicants table
    applicants_tbl.update(applicant_id, {"Compressed JSON": json.dumps(compressed)})

    print(f"✅ Compressed JSON written for {applicant_id}")

def main():
    all_applicants = applicants_tbl.all()
    for record in all_applicants:
        compress_applicant(record)

if __name__ == "__main__":
    main()
