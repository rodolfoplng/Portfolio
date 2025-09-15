# Aviationstack API ETL with Daily Cron Job  
  
This file contains a Python script (`API_Aviation_Stack.py`) that extracts departure data from the **Aviationstack API**, transforms and processes it using **pandas**, and saves the dataset as a CSV file (`Departures.csv`).  
The workflow automates the ETL pipeline to run **every day at 6:00 AM** using a cron job.  
  
---  
  
## Requirements  
  
- A valid Aviationstack API key (free subscription works with limited results).  
- Python 3.8+  
- Installed dependencies:
   
```bash
pip install pandas requests
```
  
## Python Script  
  
```python
# API_Aviation_Stack.py

import requests
import pandas as pd
from pandas import json_normalize

# Extract

params = {
    'access_key': 'your_key_here', # Insert your API key
    'dep_iata' : 'GIG', # Rio de Janeiro International Airport (Galeão)
}

response = requests.get('https://api.aviationstack.com/v1/flights', params=params)

try:
    data = response.json()
except Exception as e:
    print("Error trying to read JSON:", e)


from pandas import json_normalize

# Flatten all nested fields at any depth
df = json_normalize(data['data'])

# Transform

# Codeshare mapping
# Get the rows that are codeshare (pointing to an operated flight)
ref = (
    df.loc[df["flight.codeshared.flight_icao"].notna(),
           ["flight.codeshared.flight_icao", "airline.name", "flight.number"]]
      .copy()
)

# Build the label "Airline / Flight Number" for each codeshare
ref["pair"] = ref["airline.name"].fillna("").astype(str) + " " + ref["flight.number"].astype(str)

# Aggregate by operated flight (key = flight.codeshared.flight_icao)
agg = (
    ref.groupby(ref["flight.codeshared.flight_icao"].str.upper())["pair"]
       .apply(lambda s: " / ".join(sorted(set(s))))
       .reset_index()
       .rename(columns={
           "flight.codeshared.flight_icao": "flight.icao",
           "pair": "codeshare"
       })
)

# Merge with the original DataFrame using the operated flight key (flight.icao)
df = df.merge(agg, how="left", on="flight.icao")

# Keep "codeshare" ONLY in rows where the flight is the operated one
df.loc[df["flight.codeshared.flight_icao"].notna(), "codeshare"] = pd.NA

# Handling null values
df.dropna(subset = "codeshare", inplace = True)

# Datetime formatting
# Convert to datetime
df["departure.scheduled"] = pd.to_datetime(df["departure.scheduled"])
df["departure.estimated"] = pd.to_datetime(df["departure.estimated"])
df["departure.actual"] = pd.to_datetime(df["departure.actual"])

# Creat new date and hour columns
df["scheduled_date"] = df["departure.scheduled"].dt.date
df["scheduled_time"] = df["departure.scheduled"].dt.time

df["estimated_date"] = df["departure.estimated"].dt.date
df["estimated_time"] = df["departure.estimated"].dt.time

df["actual_date"] = df["departure.actual"].dt.date
df["actual_time"] = df["departure.actual"].dt.time

# Droppping unused columns
# Keeping only departure related columns
columns_to_keep = ['flight_status', 'departure.airport',
       'departure.timezone', 'departure.iata', 'departure.icao',
       'departure.terminal', 'departure.gate', 'departure.delay',
       'arrival.airport', 'arrival.iata', 'arrival.icao',
       'airline.name', 'airline.iata', 'airline.icao', 'flight.number',
       'flight.iata', 'flight.icao',
       'codeshare', 'scheduled_date', 'scheduled_time', 'estimated_date',
       'estimated_time', 'actual_date', 'actual_time']

df = df[columns_to_keep].reset_index()

# Load

df.to_csv("Departures.csv", index = False)
```
  
Save the following code into a file called API_Aviation_Stack.py  
Or alternatively, create the file directly from the terminal (bash):  

```bash
nano API_Aviation_Stack.py
```  
Paste the code above, then save and exit  
  
## Running Manually  
  
```bash
python3 API_Aviation_Stack.py
```
  
## Automating with Cron (Linux/macOS)  
  
1. Find the Python path
```bash

which python3
# eg.: /user/bin/python3
```
  
2. Edit your crontab  
  
```bash
crontab -e
```
  
Add the following line to schedule the ETL every day at 6:00 AM:  
```bash
0 6 * * * /user/bin/python3 /absolute/path/to/API_Aviation_Stack.py >> /absolute/path/to/cron_etl.log 2>&1
```
  
3. Verify the cron job  
```bash
crontab -l
```
  
4. Check the logs  
```bash
cat /absolute/path/to/cron_etl.log
```
  
    
For a full ETL pipeline using Apache Airflow, take a look at [Airflow DAG for API ETL](https://github.com/rodolfoplng/Airflow-DAG-for-API-ETL-Process).
  
Notebook with the API extraction and analysis: [API Request and Extraction from Aviationstack](https://github.com/rodolfoplng/Portfolio/blob/main/API%20Request%20and%20Extraction%20Aviationstack.ipynb).
