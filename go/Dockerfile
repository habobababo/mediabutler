FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -mod=vendor -trimpath -ldflags="-s -w" -o /mediabutler .

FROM debian:bookworm-slim
ARG SEVENZ_TAG=25.01
ARG SEVENZ_FILE=7z2501
RUN apt-get update && apt-get install -y --no-install-recommends tzdata ffmpeg curl xz-utils ca-certificates \
 && A="$(dpkg --print-architecture)"; case "$A" in arm64) P=arm64;; *) P=x64;; esac \
 && F="${SEVENZ_FILE}-linux-${P}.tar.xz" \
 && (curl -fsSL "https://github.com/ip7z/7zip/releases/download/${SEVENZ_TAG}/${F}" -o /tmp/7z.tar.xz \
     || curl -fsSL "https://www.7-zip.org/a/${F}" -o /tmp/7z.tar.xz) \
 && mkdir /tmp/7z && tar -xf /tmp/7z.tar.xz -C /tmp/7z \
 && if [ -f /tmp/7z/7zzs ]; then cp /tmp/7z/7zzs /usr/local/bin/7zz; else cp /tmp/7z/7zz /usr/local/bin/7zz; fi \
 && chmod +x /usr/local/bin/7zz && rm -rf /tmp/7z /tmp/7z.tar.xz /var/lib/apt/lists/*
ENV TZ=Europe/Berlin
COPY --from=build /mediabutler /usr/local/bin/mediabutler
EXPOSE 8080
CMD ["mediabutler"]
