
# ETL Pipeline: Product Pricing Analytics

## Overview
This repository contains a robust, production-ready ETL pipeline for ingesting, transforming, and storing product pricing data. The solution is orchestrated with Apache Airflow, containerized using Docker Compose, and includes a comprehensive test suite for reliability and maintainability.

---

## Features
- **Automated Data Ingestion:** Fetches product data from a public API.
- **Data Transformation:** Cleans, normalizes, and enriches product data (e.g., currency conversion, flagging high prices).
- **Data Storage:** Loads transformed data into DuckDB for fast analytics.
- **Orchestration:** Airflow DAG manages ETL workflow and scheduling.
- **Containerization:** All services run in isolated Docker containers for reproducibility.
- **Testing:** Pytest-based unit and integration tests ensure code quality.
- **Linting:** Pylint enforces code style and best practices.

---

## Architecture
```
+-------------------+      +-------------------+      +-------------------+
|   Fetch Products  | -->  | Transform Data    | -->  | Load to DuckDB    |
+-------------------+      +-------------------+      +-------------------+
	   |                        |                        |
	   v                        v                        v
   Airflow DAG            Airflow DAG              Airflow DAG
	   |                        |                        |
	   v                        v                        v
   Docker Compose         Docker Compose           Docker Compose
```

---

## Quickstart
### Prerequisites
- Python 3.12+
- Docker & Docker Compose
- (Optional) Virtualenv for local development

### Local Development
```bash
# Clone the repository
$ git clone https://github.com/<your-username>/<your-repo>.git
$ cd scdk

# Create and activate a virtual environment
$ python3 -m venv venv
$ source venv/bin/activate

# Install dependencies
$ pip install -r requirements.txt

# Run tests
$ pytest -vv tests/

# Lint code
$ PYTHONPATH=. pylint etl/ tests/
```

### Run with Docker Compose
```bash
# Build and start all services
$ docker compose up --build

# Access Airflow UI
Visit http://localhost:8080 (default admin: admin/admin)
```

---

## Airflow DAG
- Located in `airflow/dags/etl_pipeline.py`
- Orchestrates fetch, transform, and load tasks
- Easily extensible for new data sources or transformations

---

## Data Flow
1. **Fetch:** Downloads product data from API and stores as JSON/Parquet.
2. **Transform:** Cleans and enriches data (e.g., currency conversion, category normalization).
3. **Load:** Writes cleaned data to DuckDB for analytics.

---

## Testing & Quality
- **Unit tests:** `tests/unit/`
- **Integration tests:** `tests/integration/`
- **Linting:** Pylint configuration for style and import order
- **Continuous Integration:** Ready for GitHub Actions workflow (pytest, lint, Docker build)

---

## Extensibility
- Add new data sources by extending `fetch_data.py`
- Add new transformations in `transform_data.py`
- Add new storage targets in `load_to_db.py`
- Add new Airflow tasks in `airflow/dags/etl_pipeline.py`

---

## Professional Practices
- Modular, testable code with clear separation of concerns
- Containerized for reproducibility and easy deployment
- Automated testing and linting for reliability
- Well-documented for onboarding and collaboration

---

## License
MIT License

---

## Maintainer
**Micky Yansah**  
Senior Data Engineer

---

## Contact
For questions, feature requests, or collaboration, please open an issue or reach out via GitHub.
