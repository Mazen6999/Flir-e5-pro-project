# Use Python 3.11 slim image
FROM python:3.11-slim

# Install system dependencies
# Added: libgomp1 (needed for parallel processing in the thermal SDK)
RUN apt-get update && apt-get install -y \
    exiftool \
    libgl1 \
    libglib2.0-0 \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script
COPY main.py .

# Create a directory for images
RUN mkdir -p /app/images

# Command to run the script
CMD ["python", "main.py"]