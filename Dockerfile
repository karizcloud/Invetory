FROM python:3.11-slim
WORKDIR /opt/project
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONPATH=/opt/project PYTHONUNBUFFERED=1
