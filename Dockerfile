FROM python:3.10-slim

# Install requirements
COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

RUN apt-get update && apt-get install -y ncat
