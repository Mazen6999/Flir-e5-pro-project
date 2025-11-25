FROM python:3.11-slim

# Install ExifTool and system libraries needed for Thermal SDK/OpenCV
RUN apt-get update && apt-get install -y \
    exiftool \
    libgl1 \
    libglib2.0-0 \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Create the folder where images will be mounted
RUN mkdir -p /app/images

CMD ["python", "main.py"]