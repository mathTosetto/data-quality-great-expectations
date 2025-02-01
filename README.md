# 🚕 Great Expectations - Taxi Data Validation

This project leverages **Great Expectations** to validate **NYC Taxi Data**, ensuring data quality and consistency.  
It integrates **Poetry** for dependency management and **Pytest** for testing.

---

## 📌 **Project Overview**
The goal is to:
- ✅ Load NYC Taxi data from a remote source.
- ✅ Define **Great Expectations** validation rules.
- ✅ Run **data quality checks** using checkpoints.
- ✅ Generate **data documentation reports**.
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
cd data-quality-great-expectations-project
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

## 🏃‍♂️ Running the Project
### 1️⃣ Run the Data Validation Pipeline
Execute the main script to:
- Download & validate NYC Taxi data.
- Run Great Expectations validation.
- Generate a validation report.

Run:
```bash
python src/main.py
```
