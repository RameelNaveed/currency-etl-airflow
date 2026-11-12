# Currency Exchange ETL Pipeline (Astro CLI, Airflow, MongoDB)

Daily EUR exchange rates are pulled from the Frankfurter API, reshaped, and loaded into MongoDB. Mongo Express runs as a separate Docker service for browsing the data.

```
extract_exchange_rates  >>  transform_exchange_rates  >>  load_to_mongodb
   (Frankfurter API)          (named XCom in/out)          (MongoDB upsert)
```

## Project structure

```
currency-etl-airflow/
├── dags/
│   └── currency_exchange_etl.py     # the ETL DAG (3 tasks, named XComs)
├── tests/dags/test_currency_etl.py  # DAG import and task order test
├── docker-compose.override.yml      # MongoDB + Mongo Express, network, volumes
├── Dockerfile                       # Astro Runtime image + LABEL metadata
├── requirements.txt                 # pymongo, requests
├── .env.example                     # Mongo connection settings (copy to .env)
└── README.md
```

## Step by step

### 1. Prerequisites
Install and open Docker Desktop, then install Astro CLI.

```powershell
# Windows
winget install -e --id Astronomer.Astro
# macOS
brew install astro

astro version
docker --version
```

### 2. Create the Astro project

```powershell
mkdir currency-etl-airflow
cd currency-etl-airflow
astro dev init
```

Delete the sample DAG that `astro dev init` creates (`dags/exampledag.py`).

### 3. Add the project files
Copy these files from this repo into the project, replacing any generated ones:

* `dags/currency_exchange_etl.py`
* `docker-compose.override.yml`
* `requirements.txt`
* `.env` (copy of `.env.example`)
* `Dockerfile`: keep your generated `FROM` line and add the `LABEL` block

### 4. Start everything

```powershell
astro dev start
```

| Service | URL | Login |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin if asked |
| Mongo Express | http://localhost:8081 | admin / admin123 |
| MongoDB | localhost:27017 | admin / admin123 |

### 5. Check containers, network and storage

```powershell
docker ps
docker network ls
docker network inspect currency_mongo_network
docker volume ls
docker volume inspect currency_mongo_data
docker inspect currency_mongo --format "{{json .Config.Labels}}"
```

### 6. Run the DAG
In the Airflow UI open `currency_exchange_etl`, unpause it and click Trigger. All three tasks should turn green.

Check each task's Logs and XCom tabs:
* `extract_exchange_rates` pushes key `raw_exchange_rates`
* `transform_exchange_rates` pushes key `transformed_exchange_rates`

### 7. Verify the data in MongoDB
Mongo Express: open `currency_db`, then `exchange_rates`.

Or from the shell:

```powershell
docker exec -it currency_mongo mongosh -u admin -p admin123 --authenticationDatabase admin
use currency_db
db.exchange_rates.find().pretty()
db.exchange_rates.countDocuments()
```

### 8. Prove persistence

```powershell
astro dev restart
```

Open Mongo Express again; the documents are still there because `/data/db` sits on the named volume `currency_mongo_data`.

### 9. Push to GitHub

```powershell
git init
git add .
git commit -m "Airflow ETL: Frankfurter exchange rates to MongoDB"
git branch -M main
git remote add origin https://github.com/<your-username>/currency-etl-airflow.git
git push -u origin main
```

For DagsHub, create a repo there and use its URL as `origin` instead.

### 10. Stop

```powershell
astro dev stop     # keeps data
astro dev kill     # removes Airflow containers and metadata
```

## Output document format

```json
{
  "base_currency": "EUR",
  "target_currency": "USD",
  "exchange_rate": 1.1189,
  "rate_date": "2026-11-11",
  "source": "Frankfurter API",
  "retrieved_at": "2026-11-11T18:22:35"
}
```

A unique index on `(base_currency, target_currency, rate_date)` plus upserts means rerunning the DAG on the same day updates documents instead of duplicating them.

## Troubleshooting

* Port 27017 or 8081 already in use: change the left side of the port mapping in `docker-compose.override.yml`, for example `"27018:27017"`.
* Load task cannot reach Mongo: run `docker network inspect` and confirm `currency_mongo` and the scheduler are both on the Airflow network.
* Mongo Express keeps restarting: it waits for MongoDB's healthcheck; give it 20 to 30 seconds after `astro dev start`.
