# 🚕 Great Expectations - Taxi Data Validation

This project leverages **Great Expectations** to validate **NYC Taxi Data**, ensuring data quality and consistency.  

---

## 📌 **Project Overview**
![wap_archtecture](diagrams/wap_architecture.png)

The goal is to:
- ✅ Load NYC Taxi data and insert it into a staging table.
- ✅ Run Great Expectations validation rules.
- ✅ If all validations pass, push the data to the destination table.
- ✅ Generate data documentation reports.
- ✅ Automate the entire process with Python scripts.

---

## ⚡ **Installation & Setup**
### 1️⃣ **Install Poetry** (if not installed)
```sh
pip install poetry
```

### 2️⃣ Clone the Repository
Run the following command to clone this project:
```bash
git clone https://github.com/mathTosetto/data-quality-great-expectations.git
cd <path/to/your/folder>
```

### 3️⃣ Set Up the Environment
Run the following command to install dependencies and set up the virtual environment:
```bash
make install
```

This will:
- Create a Poetry virtual environment.
- Install all dependencies from pyproject.toml.

### 4️⃣ Activate the Environment
Run this command to activate the Poetry virtual environment:
```bash
eval $(poetry env activate)
```

---

## 🚀 Setting Up PostgreSQL with Docker

### 1️⃣ Configure the .env File
Before running the services, create a `.env` file in the root directory and define the following environment variables:
```ini
# PostgreSQL Configuration
POSTGRES_SCHEMA=postgres
POSTGRES_USER=postgres
POSTGRES_DB=postgres
POSTGRES_PASSWORD=postgres

# Ports Configuration
HOST_PORT=5432
CONTAINER_PORT=5432

# Docker Configurations
DOCKER_CONTAINER=my-postgres-container
DOCKER_IMAGE=my-postgres-image

# pgAdmin Configuration
PGADMIN_EMAIL=postgres@postgres.com
PGADMIN_PASSWORD=postgres
PGADMIN_PORT=5050

# Connection String
CONNECTION_STRING=postgresql://postgres:postgres@localhost:5432/postgres
```

### 2️⃣ Start the Services
Once the `.env` file is set, start the PostgreSQL and pgAdmin services using:
``` bash
make up
```

### 3️⃣ Access pgAdmin
After starting the services, open pgAdmin in your browser:

- URL: http://localhost:5050
- Login: Use the email and password set in `.env` (PGADMIN_EMAIL and PGADMIN_PASSWORD).

At the connection creation, use:
- Host name/address: host.docker.internal
- Port: 5432

### 4️⃣ Stop the Services
To stop and remove the containers, run:
```bash
make down
```

---

## 🏃‍♂️ Running the Project
### 1️⃣ Run the Data Validation Pipeline
Execute the main script to:
- ✅ Downloading NYC Taxi data.
- ✅ Running Great Expectations validation rules.
- ✅ Generating a validation report.
- ✅ Moving validated data to the production table.

Once all set, run:
```bash
python src/main.py
```
