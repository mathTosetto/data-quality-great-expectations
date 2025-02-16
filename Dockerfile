FROM apache/airflow:2.10.5

USER root
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    python3-dev \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Set user
USER airflow

# Install poetry
RUN pip install --upgrade pip && pip install poetry

# Copy project files
COPY pyproject.toml poetry.lock /opt/airflow/

# Set working directory
WORKDIR /opt/airflow

# Configure poetry to install dependencies inside the project
RUN poetry config virtualenvs.in-project true

# Install dependencies
RUN poetry install --no-root
