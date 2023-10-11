# Use the official Python 3.9 image as base
FROM python:3.9

# Set working directory
WORKDIR /app

# Copy required files
COPY . /app

# Install dependencies using Poetry
RUN pip install --no-cache-dir poetry \
 && poetry config virtualenvs.create false \
 && poetry install --no-dev

COPY --from=openjdk:8-jre-slim /usr/local/openjdk-8 /usr/local/openjdk-8

ENV JAVA_HOME /usr/local/openjdk-8

RUN update-alternatives --install /usr/bin/java java /usr/local/openjdk-8/bin/java 1

# Check Poetry version
RUN poetry --version
RUN echo "$PATH"

# Copy the JDBC driver JAR to the Spark jars directory
COPY ./jdbc_driver /opt/bitnami/spark/jars/

# Command to run the application using Poetry
CMD ["poetry", "run", "python", "main.py", "--source_file_path", "/opt/data/transaction.csv", "--database", "postgres", "--table", "customers"]