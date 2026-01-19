# --------------------------------------------------
# Base image
# --------------------------------------------------
# We use the official Python slim image:
# - Smaller size than full Python image
# - Enough to run dbt and its dependencies
FROM python:3.12-slim


# --------------------------------------------------
# Set working directory inside the container
# --------------------------------------------------
# All commands (RUN, CMD, COPY) will execute from this path
# This keeps the container filesystem organized
WORKDIR /usr/app


# --------------------------------------------------
# Install dbt with Postgres adapter
# --------------------------------------------------
# dbt is installed via pip
# dbt-postgres includes:
#   - dbt-core
#   - Postgres database adapter
# --no-cache-dir keeps the image small
RUN pip install --no-cache-dir dbt-postgres


# --------------------------------------------------
# Copy project files into the container
# --------------------------------------------------
# Copies the entire dbt project, CSVs, and config files
# into the container's working directory
COPY . /usr/app


# --------------------------------------------------
# Configure dbt profiles location
# --------------------------------------------------
# By default dbt looks for profiles.yml in ~/.dbt
# In Docker, we explicitly tell dbt where profiles.yml lives
ENV DBT_PROFILES_DIR=/usr/app/dbt


# --------------------------------------------------
# Default container command
# --------------------------------------------------
# When the container starts, this command is executed
# Can be overridden in docker-compose.yml
CMD ["dbt", "run"]
