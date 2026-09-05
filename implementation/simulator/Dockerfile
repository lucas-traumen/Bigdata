FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Non-root runtime user
RUN useradd -m -u 1000 sim && chown -R sim:sim /app
USER sim

# Long-running producer by default; override RATE/DURATION/FAULT_INJECT via env.
CMD ["python", "producer.py"]
