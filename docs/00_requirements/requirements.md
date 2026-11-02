Project Title: End-to-End Data Engineering Project – Wistia Video Analytics

🧠 Business Objective
The marketing team uses Wistia to track video engagement across Facebook and YouTube. We aim to:
	•	Collect media-level and visitor-level analytics from Wistia’s Stats API
	•	Build an automated data pipeline to ingest and analyze performance
	•	Use these insights to improve marketing strategies
This project simulates a real-world data engineering assignment with full responsibility placed on the student team to design, implement, and operate the system.

🔍 Project Scope
You will be responsible for:
	•	Designing the entire system architecture: ingestion, storage, processing, and reporting
	•	Authenticating and ingesting data from the Wistia Stats API (both media and visitor level)
	•	Handling pagination and incremental data pulls
	•	Running the pipeline in production mode for 7 days
	•	Implementing CI/CD using GitHub.
	•	Documenting decisions, assumptions, and tradeoffs 

🛠️ Technical Constraints & Freedom
	•	You must design the architecture yourself and present it to SME for approval
	•	DO NOT USE DBT FOR TRANSFORMATION.
	•	DO NOT USE ANYTHING APART FROM AWS/Azure.
	•	You must use GitHub for version control and CI/CD
	•	You must use Python for API ingestion and PySpark for data transformation.
	•	The pipeline should run for 7 days. 


🔑 Credentials
Wistia API Documentation: https://docs.wistia.com/reference/get_stats-medias-mediaid
Wistia API token.(DO NOT SHARE) <REDACTED: stored in .env as WISTIA_API_TOKEN>
Media Ids to be used: 8hunphufxp & 9k4tbcdfg0 

🐍 Example Python Snippet to Connect to Wistia
import requests

# Wistia API Configuration
API_TOKEN = "<REDACTED: stored in .env as WISTIA_API_TOKEN>"  
MEDIA_ID = "v08dlrgr7v"  # The given media ID

# Wistia Stats API Endpoint
url = f"https://api.wistia.com/v1/stats/medias/{MEDIA_ID}.json"

# API Headers
headers = {
    "Authorization": f"Bearer {API_TOKEN}"
}

# Make API Request
response = requests.get(url, headers=headers)

# Handle Response
if response.status_code == 200:
    stats = response.json()
    print("✅ Video Stats Retrieved Successfully:\n")
    print(stats)
elif response.status_code == 401:
    print("❌ Unauthorized: Check your API token permissions.")
elif response.status_code == 404:
    print("❌ Error: Media not found. Check if the media ID is correct.")
else:
    print(f"⚠️ Error: Received status code {response.status_code} - {response.text}")


✅ Requirements Summary
ID
Requirement
FR1
Design your own architecture for ingestion, processing, and storage
FR2
Authenticate to Wistia Stats API using token-based Basic Auth
FR3
Extract media metadata (title, ID, hashed_id, created_at, etc.)
FR4
Extract engagement metrics (plays, play rate, watch time, etc.)
FR5
Extract visitor-level data (IP, engagement events)
FR6
Implement pagination to fetch all pages of results
FR7
Implement incremental ingestion based on created_at/updated_at
FR8
Run this pipeline in "production mode" for 7 consecutive days
FR9
Implement a CI/CD pipeline using GitHub Actions or equivalent
FR10
Store results in a structured data model (DWH or cloud database)
FR11
Create final reports or dashboards for insights (optional)
FR12
Submit a GitHub repo with documentation, pipeline code, CI/CD setup, and instructions

📊 Data Model (Simplified)
dim_media
	•	media_id 
	•	title 
	•	url 
	•	channel (Facebook, YouTube) 
	•	created_at 
dim_visitor
	•	visitor_id 
	•	ip_address 
	•	country 
fact_media_engagement
	•	media_id 
	•	visitor_id 
	•	date 
	•	play_count 
	•	play_rate 
	•	total_watch_time 
	•	watched_percent 


📅 Suggested Milestones
Week
Milestone
1
API Exploration + Authentication
2
Extract & stage raw data
3
Design & build DWH schema
4
Load data into dimensional model
5
Build dashboard + write final report
6
Submit GitHub repo + recorded walkthrough

🧠 Evaluation Criteria
Area
Criteria
Architecture
Clear, scalable, modular design
Data Quality
Correct use of pagination, incremental logic, and schema
Engineering
Effective error handling, retries, logging
CI/CD
Working CI/CD for deployment or validation
Documentation
README + architecture diagram + setup instructions

