# Use the official Python image from the Docker Hub
FROM python:3.12-alpine

# Install git and PostgreSQL development dependencies
RUN apk add --no-cache git postgresql-dev build-base

# Set the working directory to /app
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container at /app
COPY . .

# Specify the command to run on container start
CMD ["python", "main.py"]