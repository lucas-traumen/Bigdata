FROM python:3.12-slim-bookworm

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY bridge.py ./

# SIGTERM must reach the Python process for graceful drain + flush
STOPSIGNAL SIGTERM

CMD ["python", "-u", "bridge.py"]
