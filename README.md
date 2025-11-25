# 📊 E-Commerce Analytics ELT Pipeline

This repository showcases a professional, Python-centric Extract, Load, and Transform (ELT) pipeline, designed as a comprehensive portfolio piece for an aspiring Analytics Engineer or Junior Data Engineer.

The pipeline demonstrates mastery of data movement, infrastructure, and quality assurance by transforming raw, messy e-commerce data into a clean, analytical-ready dataset.

---

## 🚀 Project Goal

The primary objective is to demonstrate proficiency in core AE practices:

- **Data Ingestion:** Securely extracting data from a relational database source.  
- **Data Quality:** Defining and enforcing a clean data schema to reject or fix malformed records.  
- **Transformation:** Merging disparate data sources and deriving new metrics (feature engineering).  
- **Automated Testing:** Proving data integrity through a dedicated, automated test suite.  
- **Observability:** Implementing robust logging and version control for monitoring and collaboration.

---

## 🛑 Data Issues Solved by the Pipeline

The raw data was intentionally seeded with real-world quality issues to test the pipeline's robustness:

- **Duplicates:** An identical `order_id` record was inserted, requiring deduplication.  
- **Bad Foreign Keys:** An order was placed with a non-existent customer_key (`CUST_999`).  
- **Data Type / Logic Errors:** Negative prices (e.g., `-10.00`) and missing quantities (`NULL`).  
- **Completeness:** Missing customer region values.

---

## 💻 Technology Stack

| Component       | Technology              | Purpose |
|-----------------|-------------------------|---------|
| Data Source     | PostgreSQL (v15)        | Relational database hosting the initial raw, "dirty" data tables. |
| Orchestration   | Python                  | Executes extraction, transformation, and loading scripts. |
| Containers      | Docker & Docker Compose | Manage and run PostgreSQL locally in an isolated, repeatable environment. |
| Extraction      | SQLAlchemy & Pandas     | Connects to Postgres and loads data into DataFrames. |
| Data Quality    | Pydantic                | Defines strict data schemas (data contracts) during transformation. |
| Data Testing    | Pytest                  | Automated tests validating final data integrity. |
| Version Control | Git & GitHub            | Manages project history, collaboration, and deployment readiness. |

---

## 🗓️ Day-by-Day Progress

### **Phase 1: Setup & Data Ingestion (Day 1 & 2 Complete)**  
This phase established the environment, created the raw data source, and implemented the data extraction layer.

---

### **Day 1: Environment and Source Data Generation**

| File(s)               | Function | Key Skills |
|----------------------|----------|------------|
| `docker-compose.yml` | Defines the PostgreSQL service container for a repeatable local database environment. | Infrastructure as Code (IaC), Docker |
| `.env` & `.gitignore` | Securely stores sensitive database credentials and ignores non-essential files. | Security, Environment Management |
| `generate_source_data.py` | Populates `raw_customers` and `raw_orders` with 1,000 intentionally messy records. | Data Generation, SQL DDL, Pandas |

---

### **Day 2: Extraction and Observability**

| File(s)       | Function | Key Skills |
|---------------|----------|------------|
| `extractor.py` | Performs the Extract (E) step by connecting to PostgreSQL and pulling raw data into Pandas DataFrames. | Data Extraction, SQL |
| `pipeline.log` | Logs events to both terminal and file for observability. | Logging, Monitoring |
| Git Commit     | Initializes repository, adds code files, pushes to GitHub. | Git/GitHub |

---

### **Day 3: Transformation and Pydantic Validation (Complete)**

This phase implemented the core data cleaning and quality enforcement layer, addressing all known data issues.

| File(s)       | Function | Key Skills |
|---------------|----------|------------|
| `schemas.py`  | Defines the output data structure using a Pydantic `CleanedOrder` model with custom validators. | Data Quality, Schema Design, Data Contracts |
| `cleaner.py`  | Merges data, deduplicates records, imputes missing regions, fixes negative prices, derives `total_sale`, and validates each record via Pydantic. | Pandas, Data Integrity, Validation |

---

### **Day 4 & 5: Loading and Automated Testing (Complete)**

This phase completes the ELT loop by loading the clean data into the final reporting table and running an automated test suite to guarantee data integrity.

| File(s)        | Function | Key Skills |
|----------------|----------|------------|
| `loader.py`    | Orchestrates the full E-T-L pipeline and loads validated data into the `analytics_sales` table. | Pipeline Orchestration, Data Loading, Error Handling |
| `test_pipeline.py` | Runs Pytest checks: no duplicates, no negative values, valid foreign keys, correct row counts. | Automated Testing, Data Validation |
| `test_cleaner.py` | Unit Test: Uses Pytest with mock Pandas DataFrames to perform fast, isolated tests on the core logic in cleaner.py, ensuring functions work reliably without database access. | Unit Testing, Test Isolation, Mocking Data |

---

## 🛠️ Getting Started (Local Setup)

To run this project locally:

---

### **1. Clone the Repository**
```bash
git clone https://github.com/shahidmalik4/ecommerce-analytics-elt.git
cd ecommerce-analytics-elt
```

### **2. Ensure Docker is Running**

### **3. Start the Database**
```bash
docker compose up -d
```

### **4. Generate Source Data**
```bash
python generate_source_data.py
```

### **5. Run the Full Pipeline (E → T → L)**
```bash
python loader.py
```

### **6. Run Automated Data Validation (QA)**
```bash
pytest
```





