FROM python:3.12-slim

ARG VERSION=v0.1.0
LABEL org.opencontainers.image.title="bkvue" \
      org.opencontainers.image.version="${VERSION}"

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

ENV PORT=8080
EXPOSE 8080
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "--threads", "8", "app:app"]
