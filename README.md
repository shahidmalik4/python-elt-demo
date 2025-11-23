# 📊 E-Commerce Analytics ELT Pipeline

This repository showcases a professional, Python-centric Extract, Load, and Transform (ELT) pipeline, designed as a comprehensive portfolio piece for an aspiring Analytics Engineer or Junior Data Engineer.

The pipeline demonstrates mastery of data movement, infrastructure, and quality assurance by transforming raw, messy e-commerce data into a clean, analytical-ready dataset.

---

## 🚀 Project Goal

The primary objective is to demonstrate proficiency in core AE practices:

- **Data Ingestion:** Securely extracting data from a relational database source.  
- **Data Quality:** Defining and enforcing a clean data schema to reject or fix malformed records.  
- **Transformation:** Merging disparate data sources and deriving new metrics (feature engineering).  
- **Observability:** Implementing robust logging and version control for monitoring and collaboration.

---

## 💻 Technology Stack

| Component       | Technology            | Purpose |
|-----------------|-----------------------|---------|
| Data Source     | PostgreSQL (v15)      | Relational database hosting the initial raw, "dirty" data tables. |
| Orchestration   | Python                | Main language used for running extraction and transformation scripts. |
| Containers      | Docker & Docker Compose | Manage and run PostgreSQL locally in an isolated, repeatable environment. |
| Extraction      | SQLAlchemy & Pandas   | Libraries used to connect to Postgres and load data into DataFrames. |
| Data Quality    | Pydantic              | Defines strict data schemas (data contracts) during transformation. |
| Version Control | Git & GitHub          | Manages project history, collaboration, and deployment readiness. |

---

## 🗓️ Day-by-Day Progress

### **Phase 1: Setup & Data Ingestion (Day 1 & 2 Complete)**  
This phase established the environment, created the raw data source, and implemented the data extraction layer.

---

### **Day 1: Environment and Source Data Generation**

| File(s)               | Function | Key Skills |
|----------------------|----------|------------|
| `docker-compose.yml` | Defines the PostgreSQL service container for a repeatable local database environment. | Infrastructure as Code (IaC), Docker |
| `.env` & `.gitignore` | Securely stores sensitive database credentials and ignores non-essential files (logs, env files). | Security, Environment Management |
| `generate_source_data.py` | Connects to the empty DB and populates `raw_customers` and `raw_orders` with 1,000 intentional dirty records. | Data Generation, SQL DDL, Pandas |

---

### **Day 2: Extraction and Observability**

| File(s)       | Function | Key Skills |
|---------------|----------|------------|
| `extractor.py` | Performs the Extract (E) step by connecting to PostgreSQL and pulling raw data into Pandas DataFrames. | Data Extraction, SQL |
| `pipeline.log` | Implements robust logging to log events to both terminal and file for observability. | Logging, Monitoring |
| Git Commit     | Initializes repository, adds code files, pushes to GitHub. | Git/GitHub |

---

## 🛠️ Getting Started (Local Setup)

To run this project locally:

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

### **5. Test Extraction**
```bash
python extractor.py
```






