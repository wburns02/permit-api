FROM python:3.12.10-slim

WORKDIR /app

RUN apt-get -o Acquire::Retries=6 update && apt-get -o Acquire::Retries=6 install -y --fix-missing \
    gcc \
    libpq-dev \
    curl \
    socat \
    iptables \
    && curl -fsSL https://tailscale.com/install.sh | sh \
    && curl -fsSL https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o /usr/local/bin/cloudflared \
    && chmod +x /usr/local/bin/cloudflared \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ARG CACHEBUST=1
COPY . .

RUN chmod +x start.sh

EXPOSE 8080

CMD ["./start.sh"]
