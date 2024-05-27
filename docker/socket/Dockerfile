FROM python:3.10-slim

# Install some system dependencies
RUN apt-get update && apt-get -y dist-upgrade
RUN apt-get install -y \
    gcc \
    libpq-dev
RUN apt install -y netcat-traditional

# Install the required packages
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy the source code
COPY ./workdir/ /app
WORKDIR /app

# Run the application
ENTRYPOINT ["python", "opensea_client.py"]
