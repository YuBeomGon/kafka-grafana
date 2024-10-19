# Use official Python image
FROM python:3.9-slim

# Set working directory in the container
WORKDIR /app

# Copy requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the producer script into the container
COPY producer.py .

# Command to run the producer script
CMD ["python", "producer.py"]