FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends tzdata ffmpeg \
 && rm -rf /var/lib/apt/lists/* && pip install --no-cache-dir flask
ENV TZ=Europe/Berlin
WORKDIR /app
COPY app.py .
EXPOSE 8080
CMD ["python", "app.py"]
