import os
import json
import time
import openai
from dotenv import load_dotenv
from pyairtable import Api

load_dotenv()

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("BASE_ID")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

openai.api_key = OPENAI_API_KEY

api = Api(AIRTABLE_API_KEY)
applicants_tbl = api.table(BASE_ID, "Applicants")

def call_llm_with_retry(prompt, max_retries=3, delay=3):
    for attempt in range(max_retries):
        try:
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a recruiting analyst."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=300,
            )
            return response['choices'][0]['message']['content']
        except Exception as e:
            print(f"⚠️ LLM call failed (attempt {attempt + 1}): {e}")
            time.sleep(delay * (2 ** attempt))
    return None

def build_prompt(json_data):
    return f"""
You are a recruiting analyst. Given this applicant JSON, do four things:
1. Provide a concise 75-word summary.
2. Rate overall candidate quality from 1-10 (higher is better).
3. List any data gaps or inconsistencies you notice.
4. Suggest up to three follow-up questions to clarify gaps.

Return exactly:
Summary: <text>
Score: <integer>
Issues: <comma-separated list or 'None'>
Follow-Ups: <bullet list>

Applicant JSON:
{json.dumps(json_data, indent=2)}
""".strip()

def update_llm_fields(record_id, response_text):
    try:
        summary = response_text.split("Summary:")[1].split("Score:")[0].strip()
        score = int(response_text.split("Score:")[1].split("Issues:")[0].strip())
        issues = response_text.split("Issues:")[1].split("Follow-Ups:")[0].strip()
        followups = response_text.split("Follow-Ups:")[1].strip()

        applicants_tbl.update(record_id, {
            "LLM Summary": summary,
            "LLM Score": score,
            "LLM Follow-Ups": followups
        })
        print(f"✅ Updated LLM fields for {record_id}")
    except Exception as e:
        print(f"❌ Failed to parse or update LLM response for {record_id}: {e}")

def main():
    applicants = applicants_tbl.all()
    for rec in applicants:
        record_id = rec["id"]
        fields = rec.get("fields", {})
        json_blob = fields.get("Compressed JSON", "")
        existing_summary = fields.get("LLM Summary")

        if not json_blob:
            print(f"⏭️ No JSON for {record_id}")
            continue

        if existing_summary:  # avoid redundant API calls
            print(f"ℹ️ Skipping {record_id}: LLM already filled.")
            continue

        try:
            data = json.loads(json_blob)
        except json.JSONDecodeError:
            print(f"❌ Invalid JSON for {record_id}")
            continue

        prompt = build_prompt(data)
        result = call_llm_with_retry(prompt)

        if result:
            update_llm_fields(record_id, result)
        else:
            print(f"❌ Skipped {record_id}: LLM failed after retries.")

if __name__ == "__main__":
    main()
