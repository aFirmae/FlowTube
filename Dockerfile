# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies
# - ffmpeg: required for audio/video processing
# - curl, gnupg: required for NodeSource setup
# - git: required to clone bgutil provider
RUN apt-get update && apt-get install -y \
    ffmpeg \
    curl \
    gnupg \
    git \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Clone and build the bgutil PO Token provider server
RUN git clone https://github.com/Brainicism/bgutil-ytdlp-pot-provider.git /opt/bgutil \
    && cd /opt/bgutil/server \
    && npm ci \
    && npx tsc

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Create the entrypoint script
RUN echo '#!/bin/bash\n\
# Start the bgutil PO Token provider in the background on port 4416\n\
cd /opt/bgutil/server && node build/main.js --port 4416 &\n\
\n\
# Wait for the provider to start\n\
sleep 2\n\
\n\
# Start the main application\n\
exec uvicorn music:app --host 0.0.0.0 --port ${PORT:-8000}\n' > /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Expose the port
EXPOSE 8000

# Run the entrypoint script
CMD ["/app/entrypoint.sh"]
