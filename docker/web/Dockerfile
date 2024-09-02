FROM python:3.10-slim

RUN pip install --upgrade pip

# Install the application
COPY . /app
WORKDIR /app

RUN pip install -e ".[web]"

ENTRYPOINT ["python", "-m", "opensea_monitoring.www.app"]
