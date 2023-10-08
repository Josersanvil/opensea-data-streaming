FROM jupyter/pyspark-notebook:hub-4.0.2

# Install requirements
COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

