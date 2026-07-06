# 📦 Deployment Configuration

This folder contains **operational and deployment-related configurations** that are separate from the core application.

## 📂 Contents

### `docker-compose.production.yml`
Production-grade Docker Compose configuration with:
- Nginx reverse proxy for HTTPS
- Container networking (app not exposed directly)
- SSL certificate mounting
- Resource limits optimized for production
- All security hardening from the main project

**Usage:**
```bash
docker-compose -f deployment/docker-compose.production.yml up -d
```

### `nginx.conf`
Nginx reverse proxy configuration for HTTPS/TLS:
- HTTP to HTTPS redirect
- TLS 1.2/1.3 support
- Security headers (HSTS, X-Frame-Options, etc.)
- WebSocket support for Watch Party
- Gzip compression

**Setup:**
1. Get SSL certificate from Let's Encrypt:
   ```bash
   sudo certbot certonly --standalone -d yourdomain.com
   ```

2. Edit `nginx.conf` - replace `yourdomain.com` with your domain

3. Use with production docker-compose:
   ```bash
   docker-compose -f deployment/docker-compose.production.yml up -d
   ```

## 🚀 Quick Production Setup

### For Development (No HTTPS)
Use the root **`docker-compose.yml`**:
```bash
docker-compose up -d
```

### For Production (With HTTPS)
Use **`deployment/docker-compose.production.yml`**:
```bash
# 1. Get SSL certificate
sudo certbot certonly --standalone -d yourdomain.com

# 2. Edit deployment/nginx.conf (replace yourdomain.com)
nano deployment/nginx.conf

# 3. Deploy with production compose
docker-compose -f deployment/docker-compose.production.yml up -d

# 4. Verify HTTPS
curl -v https://yourdomain.com/health
```

## 🔒 Security Features

✅ HTTP to HTTPS redirect  
✅ Strong TLS ciphers (1.2/1.3 only)  
✅ Security headers (HSTS, X-Frame-Options, etc.)  
✅ Gzip compression  
✅ WebSocket support  
✅ App not directly exposed  

## 🔧 Configuration

### SSL Certificates
Certificates are mounted from `/etc/letsencrypt`:
```yaml
volumes:
  - /etc/letsencrypt:/etc/letsencrypt:ro
```

### Domain Name
Edit `nginx.conf` and replace `yourdomain.com` with your actual domain (2 places):
```nginx
server_name yourdomain.com;
ssl_certificate /etc/letsencrypt/live/yourdomain.com/fullchain.pem;
ssl_certificate_key /etc/letsencrypt/live/yourdomain.com/privkey.pem;
```

### Ports
- Port 80: HTTP (redirects to 443)
- Port 443: HTTPS (main service)

## 📋 Pre-Production Checklist

- [ ] Domain name configured
- [ ] SSL certificate obtained (`certbot`)
- [ ] `nginx.conf` updated with your domain
- [ ] `.env` file configured
- [ ] All secrets generated
- [ ] Staging deployment tested

## 🧪 Testing

```bash
# Test HTTP redirect
curl -v http://yourdomain.com/health

# Test HTTPS
curl -v https://yourdomain.com/health

# Test API authentication
curl -H "Authorization: Bearer {token}" https://yourdomain.com/queue/1

# Test WebSocket (Watch Party)
wscat -c wss://yourdomain.com/ws/socket.io
```

## 🆘 Troubleshooting

### SSL Certificate Issues
```bash
# Verify certificate
sudo certbot certificates

# Renew certificate
sudo certbot renew --dry-run

# Auto-renew setup
sudo systemctl enable certbot.timer
```

### Nginx Not Starting
```bash
# Check nginx logs
docker logs emby-trakt-nginx

# Verify nginx config
docker run --rm -v $(pwd)/deployment/nginx.conf:/etc/nginx/conf.d/default.conf:ro nginx:alpine nginx -t
```

### Port Already in Use
```bash
# Check what's using ports 80/443
sudo lsof -i :80
sudo lsof -i :443

# If needed, change ports in docker-compose.production.yml
```

## 📚 Resources

- **SSL/TLS Setup:** https://certbot.eff.org/
- **Nginx Docs:** https://nginx.org/en/docs/
- **Docker Compose:** https://docs.docker.com/compose/

## ⚠️ Important Notes

1. **Not for development** - Use root `docker-compose.yml` for development
2. **Production only** - This is optimized for production deployments
3. **SSL required** - Must have valid certificate for HTTPS
4. **Domain required** - Must have a registered domain name
5. **Separate from app** - These are operational configs, not application code

---

**Choose the right setup:**
- 🏠 Local/dev: `docker-compose.yml` (root)
- 🌍 Production: `deployment/docker-compose.production.yml` + `deployment/nginx.conf`
