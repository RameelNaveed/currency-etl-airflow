# Keep the FROM line that "astro dev init" generated for you if it differs.
FROM astrocrpublic.azurecr.io/runtime:3.1-5

# Image metadata
LABEL maintainer="Rameel" \
      project="currency-etl-airflow" \
      description="Airflow ETL: Frankfurter API exchange rates to MongoDB" \
      version="1.0"
