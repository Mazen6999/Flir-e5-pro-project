# Use a lightweight Python base
FROM python:3.9-slim

# 1. Install System Dependencies
# 'exiftool' is REQUIRED for flirpy to read thermal metadata
# 'libgl1' and 'libglib' are often needed for OpenCV
RUN apt-get update && apt-get install -y \
    exiftool \
    libgl1-mesa-glx \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# 2. Install Python libraries
RUN pip install --no-cache-dir \
    flirpy \
    numpy \
    opencv-python-headless

# 3. Set working directory
WORKDIR /app

# 4. CRITICAL: Copy ALL files (the script AND the .jpg)
# This takes everything in your Windows folder and puts it in /app
COPY . .

# 5. Run the script
CMD ["python", "thermal_script.py"]