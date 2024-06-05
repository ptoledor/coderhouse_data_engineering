FROM apache/airflow:2.3.3

# Create DAGs directory
WORKDIR /opt/airflow/dags

# Copy the DAG Python file
COPY dag.py

# Set the default Airflow home directory
ENV AIRFLOW_HOME /opt/airflow

# Expose Airflow webserver port
EXPOSE 8080

# Run Airflow scheduler and webserver
CMD ["airflow", "scheduler"]
CMD ["airflow", "webserver"]

# Install python libraries
RUN pip install pandas