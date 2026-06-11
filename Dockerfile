# Digest-pinned: tag overwrites on Docker Hub cannot substitute a different image.
FROM python:3.12.10-slim@sha256:fd95fa221297a88e1cf49c55ec1828edd7c5a428187e67b5d1805692d11588db

WORKDIR /app

# Pinned versions: update by editing these two ARGs and their SHA256s below.
ARG TAILSCALE_VERSION=1.98.4
ARG CLOUDFLARED_VERSION=2026.6.0

# sha256 for tailscale_${TAILSCALE_VERSION}_amd64.tgz
# Source: https://pkgs.tailscale.com/stable/tailscale_${TAILSCALE_VERSION}_amd64.tgz.sha256
ARG TAILSCALE_SHA256=e6c08a8ee7e63e69aaf1b62ecd12672b3883fbcd2a176bf6cfa42a15fdce0b6b

# sha256 for cloudflared-linux-amd64 ${CLOUDFLARED_VERSION}
# Source: sha256sum of https://github.com/cloudflare/cloudflared/releases/download/${CLOUDFLARED_VERSION}/cloudflared-linux-amd64
ARG CLOUDFLARED_SHA256=08d27c4c5d3ed73ee3e98ef2ddceb4ad09fd4cfc28e243565a189538e8ccd706

RUN apt-get -o Acquire::Retries=6 update && apt-get -o Acquire::Retries=6 install -y --fix-missing \
    gcc \
    libpq-dev \
    curl \
    socat \
    iptables \
    # --- Tailscale: pinned static binary, verified before install -------------
    && curl -fsSL "https://pkgs.tailscale.com/stable/tailscale_${TAILSCALE_VERSION}_amd64.tgz" \
         -o /tmp/tailscale.tgz \
    && echo "${TAILSCALE_SHA256}  /tmp/tailscale.tgz" | sha256sum -c - \
    && tar -C /tmp -xzf /tmp/tailscale.tgz \
    && install -m 0755 "/tmp/tailscale_${TAILSCALE_VERSION}_amd64/tailscale"  /usr/local/bin/tailscale \
    && install -m 0755 "/tmp/tailscale_${TAILSCALE_VERSION}_amd64/tailscaled" /usr/local/bin/tailscaled \
    && rm -rf /tmp/tailscale.tgz "/tmp/tailscale_${TAILSCALE_VERSION}_amd64" \
    # --- cloudflared: pinned release, verified before install -----------------
    && curl -fsSL "https://github.com/cloudflare/cloudflared/releases/download/${CLOUDFLARED_VERSION}/cloudflared-linux-amd64" \
         -o /usr/local/bin/cloudflared \
    && echo "${CLOUDFLARED_SHA256}  /usr/local/bin/cloudflared" | sha256sum -c - \
    && chmod +x /usr/local/bin/cloudflared \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ARG CACHEBUST=1
COPY . .

RUN chmod +x start.sh

EXPOSE 8080

CMD ["./start.sh"]
