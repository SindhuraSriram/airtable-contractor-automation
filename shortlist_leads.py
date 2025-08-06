import os
import json
from pyairtable import Api
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("BASE_ID")

api = Api(API_KEY)

applicants_tbl = api.table(BASE_ID, "Applicants")
shortlist_tbl = api.table(BASE_ID, "Shortlisted Leads")

TIER1_COMPANIES = ["Google", "Meta", "OpenAI", "Apple", "Microsoft", "Amazon"]
VALID_LOCATIONS = ["US", "United States", "Canada", "UK", "United Kingdom", "Germany", "India"]

def evaluate_applicant(applicant):
    applicant_id = applicant["id"]
    fields = applicant.get("fields", {})
    compressed = fields.get("Compressed JSON")

    if not compressed:
        print(f"⏭️ Skipping {applicant_id}: No compressed JSON.")
        return

    try:
        data = json.loads(compressed)
    except json.JSONDecodeError:
        print(f"❌ Invalid JSON for {applicant_id}")
        return

    personal = data.get("personal", {})
    experience = data.get("experience", [])
    salary = data.get("salary", {})

    # --- Rule 1: Experience ---
    tier1_flag = any(e.get("company", "").strip() in TIER1_COMPANIES for e in experience)
    years = len(experience)
    experience_ok = years >= 4 or tier1_flag

    # --- Rule 2: Compensation ---
    rate = salary.get("rate")
    availability = salary.get("availability")
    compensation_ok = rate is not None and availability is not None and rate <= 100 and availability >= 20

    # --- Rule 3: Location ---
    location = personal.get("location", "").strip()
    location_ok = any(loc.lower() in location.lower() for loc in VALID_LOCATIONS)

    # --- Evaluate ---
    if experience_ok and compensation_ok and location_ok:
        reason = []

        if tier1_flag:
            reason.append("Worked at Tier-1 company")
        elif years >= 4:
            reason.append("≥ 4 years experience")

        reason.append(f"Preferred Rate: {rate} USD/hour")
        reason.append(f"Availability: {availability} hrs/week")
        reason.append(f"Location: {location}")

        score_reason = "; ".join(reason)

        # Check if already shortlisted
        existing = shortlist_tbl.all(formula=f"{{Applicant}} = '{applicant_id}'")
        if not existing:
            shortlist_tbl.create({
                "Applicant": [applicant_id],
                "Compressed JSON": compressed,
                "Score Reason": score_reason,
            })

            applicants_tbl.update(applicant_id, {"Shortlist Status": "Shortlisted"})
            print(f"✅ Shortlisted: {applicant_id} — {score_reason}")
        else:
            print(f"ℹ️ Already shortlisted: {applicant_id}")
    else:
        print(f"❌ Not shortlisted: {applicant_id}")

def main():
    applicants = applicants_tbl.all()
    for applicant in applicants:
        evaluate_applicant(applicant)

if __name__ == "__main__":
    main()
