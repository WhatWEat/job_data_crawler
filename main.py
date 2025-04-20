import argparse
import requests
import csv
import time
from tqdm import tqdm
import os
def fetch_job_listings(limit=20):
    base_url = "https://pultegroup.wd1.myworkdayjobs.com/wday/cxs/pultegroup/PGI/jobs"
    offset = 0
    all_jobs = []
    total = 0
    while True:
        payload = {
            "appliedFacets": {},
            "limit": limit,
            "offset": offset,
            "searchText": ""
        }
        response = requests.post(base_url, json=payload)
        response.raise_for_status()
        data = response.json()
        if total == 0:
            total = data.get("total", 0)
            
        postings = data.get("jobPostings", [])

        if not postings:
            break

        for job in postings:
            all_jobs.append({
                "title": job.get("title", ""),
                "externalPath": job.get("externalPath", ""),
                "locationsText": job.get("locationsText", ""),
                "bulletFields": ";".join(job.get("bulletFields", []))
            })

        offset += limit
        print(f"Scraped {len(postings)} jobs, total so far: {len(all_jobs)}")
        if offset >= total:
            break

        time.sleep(1)

    return all_jobs


def fetch_job_details(external_path):
    """
    Fetch detailed job info for a single posting using its externalPath.
    Returns a dict of detailed fields.
    """
    base = "https://pultegroup.wd1.myworkdayjobs.com/wday/cxs/pultegroup/PGI"
    url = base + external_path
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json().get("jobPostingInfo", {})

    hiring_org = resp.json().get("hiringOrganization", {}).get("name", "")

    details = {
        "job_id": data.get("id", ""),
        "location": data.get("location", ""),
        "posted_on": data.get("postedOn", ""),
        "start_date": data.get("startDate", ""),
        "time_type": data.get("timeType", ""),
        "job_req_id": data.get("jobReqId", ""),
        "job_posting_id": data.get("jobPostingId", ""),
        "country": data.get("country", {}).get("descriptor", ""),
        "requisition_location": data.get("jobRequisitionLocation", {}).get("descriptor", ""),
        "hiring_organization": hiring_org,
        "job_description": data.get("jobDescription", "")
    }
    return details


def save_csv(data, filename):
    if not data:
        print(f"No data to save to {filename}.")
        return
    fields = list(data[0].keys())
    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(data)
    print(f"Saved {len(data)} records to {filename}")


def load_csv(filename):
    """Load CSV file into list of dicts."""
    with open(filename, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [row for row in reader]


def main(update_listings=False, listings_file="listings.csv", details_file="jobs_detailed.csv", limit=20):
    if not update_listings and os.path.exists(listings_file):
        print(f"Loading listings from {listings_file}...")
        listings = load_csv(listings_file)
    else:
        print("Fetching fresh job listings...")
        listings = fetch_job_listings(limit=limit)
        save_csv(listings, listings_file)

    existing_details = {}
    if os.path.exists(details_file):
        print(f"Loading existing details from {details_file}...")
        for rec in load_csv(details_file):
            path = rec.get("externalPath")
            if path:
                existing_details[path] = rec

    combined = []
    for entry in tqdm(listings, desc="Processing listings"):
        path = entry.get("externalPath")
        if not path:
            continue
        current_bullets = entry.get("bulletFields", "")
        need_fetch = True
        
        if path in existing_details:
            if existing_details[path].get("bulletFields") == current_bullets:

                combined.append(existing_details[path])
                need_fetch = False
        if need_fetch:
            try:
                details = fetch_job_details(path)
                record = {**entry, **details}
                combined.append(record)
            except Exception as e:
                print(f"Error fetching details for {path}: {e}")
            time.sleep(1)
        save_csv(combined, details_file)


if __name__ == "__main__":
    main()