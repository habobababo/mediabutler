FROM python:3.12-slim
# 7-Zip (offizielles Binary): entpackt RAR/RAR5, ZIP, 7z, auch mehrteilig und mit Passwort
ARG SEVENZ=7z2409
RUN apt-get update && apt-get install -y --no-install-recommends tzdata ffmpeg curl xz-utils ca-certificates \
 && A="$(dpkg --print-architecture)"; case "$A" in arm64) P=arm64;; *) P=x64;; esac \
 && curl -fsSL "https://www.7-zip.org/a/${SEVENZ}-linux-${P}.tar.xz" -o /tmp/7z.tar.xz \
 && mkdir /tmp/7z && tar -xf /tmp/7z.tar.xz -C /tmp/7z \
 && if [ -f /tmp/7z/7zzs ]; then cp /tmp/7z/7zzs /usr/local/bin/7zz; else cp /tmp/7z/7zz /usr/local/bin/7zz; fi \
 && chmod +x /usr/local/bin/7zz && rm -rf /tmp/7z /tmp/7z.tar.xz /var/lib/apt/lists/* \
 && pip install --no-cache-dir flask
ENV TZ=Europe/Berlin
WORKDIR /app
COPY app.py .
EXPOSE 8080
CMD ["python", "app.py"]
