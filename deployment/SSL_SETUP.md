# SSL/TLS Setup & Automatic Renewal

This guide covers setting up HTTPS with Let's Encrypt and automatic certificate renewal.

## Prerequisites

- Docker & Docker Compose (production compose file)
- A domain name pointing to your server's public IP
- Port 80 (HTTP) accessible from the internet (for Let's Encrypt validation)

## Architecture

```
Internet → :80/:443 → nginx container
                         ├── /.well-known/acme-challenge/ → certbot-webroot volume
                         └── everything else → emby-trakt-suite:8000

certbot container (loop):
  every 12h → certbot renew --webroot → writes to certbot-webroot volume
                                       → updates certs in /etc/letsencrypt

nginx container (entrypoint):
  starts nginx + every 6h → nginx -s reload → picks up renewed certs
```

## 1. Configure Environment

Add to your `.env` file:

```bash
# Your domain (used by nginx and the app's SSL monitoring)
SSL_DOMAIN=yourdomain.com
```

## 2. Update nginx.conf

Edit `deployment/nginx.conf` and replace `yourdomain.com` with your actual domain in these lines:

```nginx
server_name yourdomain.com www.yourdomain.com;
ssl_certificate /etc/letsencrypt/live/yourdomain.com/fullchain.pem;
ssl_certificate_key /etc/letsencrypt/live/yourdomain.com/privkey.pem;
```

## 3. Start in Production Mode

```bash
cd deployment
docker-compose -f docker-compose.production.yml up -d
```

## 4. Create Initial Certificate

Nginx will start and serve HTTP on port 80 (ACME challenges pass through, everything else redirects to HTTPS). With nginx running, request the initial cert:

```bash
docker-compose -f docker-compose.production.yml exec certbot certbot certonly \
  --webroot \
  -w /var/www/certbot \
  -d yourdomain.com \
  -d www.yourdomain.com \
  --email your-email@example.com \
  --agree-tos \
  --non-interactive
```

## 5. Reload Nginx

```bash
docker-compose -f docker-compose.production.yml exec nginx nginx -s reload
```

## 6. Verify

```bash
curl https://yourdomain.com/health
```

## Automatic Renewal — How It Works

Three things happen automatically once the initial cert is in place:

1. **Certbot container** runs `certbot renew --webroot` every 12 hours. Certbot only renews when the cert is within 30 days of expiry.

2. **Nginx entrypoint** (`nginx-entrypoint.sh`) reloads nginx every 6 hours, so renewed certs are picked up without a container restart.

3. **App SSL monitoring** (if `SSL_DOMAIN` is set in `.env`) connects to the domain over TLS daily at 6 AM UTC and records: days until expiry, issuer, status. Visible on the dashboard under "🔒 SSL Certificate" and in the scheduler panel as "🔒 SSL Cert Check". Logs a warning when the cert is within 7 days of expiry.

## Dashboard Monitoring

When `SSL_DOMAIN` is set, the dashboard shows:

- **SSL Certificate card** — domain, days remaining, expiry date, issuer, status dot (🟢 >30d, 🟡 7–30d, 🔴 <7d, ⛔ expired)
- **Scheduler panel** — "🔒 SSL Cert Check" job with last-run time and status

The API endpoint `GET /api/ssl/status` returns the full cert status JSON.

## Troubleshooting

### Certificate not found on first startup

Nginx will fail to start HTTPS if the cert files don't exist yet. The HTTP block still works (port 80), so you can run the certbot command from step 4, then reload.

### ACME challenge fails

Verify port 80 is accessible from the internet and that the nginx `/.well-known/acme-challenge/` location is working:

```bash
# From outside your network:
curl http://yourdomain.com/.well-known/acme-challenge/test
# Should return 404 (not a redirect)
```

### Renewal not working

```bash
# Check certbot logs
docker-compose -f docker-compose.production.yml logs certbot

# Dry run
docker-compose -f docker-compose.production.yml exec certbot \
  certbot renew --dry-run

# Check certificate details
docker-compose -f docker-compose.production.yml exec certbot \
  certbot certificates
```

### DNS validation (if port 80 is blocked)

```bash
docker-compose -f docker-compose.production.yml exec certbot \
  certbot certonly \
  --manual \
  --preferred-challenges dns \
  -d yourdomain.com \
  --email your-email@example.com \
  --agree-tos
```

Follow the prompts to add DNS TXT records. Note: DNS challenges cannot auto-renew without a DNS plugin.

## References

- [Let's Encrypt Documentation](https://letsencrypt.org/docs/)
- [Certbot Manual](https://certbot.eff.org/docs/)
- [Nginx SSL Configuration](https://nginx.org/en/docs/http/ngx_http_ssl_module.html)
