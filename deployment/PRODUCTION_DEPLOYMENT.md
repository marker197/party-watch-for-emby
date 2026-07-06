# Production Deployment Guide

## 🚀 Production Deployment with HTTPS

This guide walks through deploying emby-trakt-suite in production with HTTPS.

### Prerequisites

- Domain name (e.g., `yourdomain.com`)
- Server with Docker and Docker Compose
- Ports 80 and 443 accessible
- Root or sudo access (for certbot)

### Step 1: Get SSL Certificate

```bash
# Install certbot (if not already installed)
sudo apt-get install certbot

# Get SSL certificate from Let's Encrypt
sudo certbot certonly --standalone -d yourdomain.com

# This creates:
# - /etc/letsencrypt/live/yourdomain.com/fullchain.pem
# - /etc/letsencrypt/live/yourdomain.com/privkey.pem
```

### Step 2: Configure nginx.conf

Edit `deployment/nginx.conf` and replace `yourdomain.com` with your actual domain:

```bash
nano deployment/nginx.conf
```

Find and replace these two lines:
```nginx
# Line ~7
server_name yourdomain.com;

# Line ~11
ssl_certificate /etc/letsencrypt/live/yourdomain.com/fullchain.pem;

# Line ~12
ssl_certificate_key /etc/letsencrypt/live/yourdomain.com/privkey.pem;
```

### Step 3: Create .env File

```bash
cp .env.example .env
nano .env
```

Required values:
```
TRAKT_CLIENT_ID=xxxxx
TRAKT_CLIENT_SECRET=xxxxx
EMBY_URL=http://192.168.1.100:8096
EMBY_API_KEY=xxxxx
DB_PASSWORD=xxxxx  # Generate: openssl rand -base64 24
REDIS_PASSWORD=xxxxx  # Generate: openssl rand -base64 24
JWT_SECRET_KEY=xxxxx  # Generate: openssl rand -hex 32
ALLOWED_ORIGINS=https://yourdomain.com
```

### Step 4: Deploy

```bash
# From root project directory
docker-compose -f deployment/docker-compose.production.yml up -d

# Wait for startup
sleep 5

# Check status
docker-compose -f deployment/docker-compose.production.yml ps
```

### Step 5: Verify

```bash
# Test HTTPS
curl -v https://yourdomain.com/health

# Test HTTP redirect to HTTPS
curl -v -L http://yourdomain.com/health

# Test API
curl -v https://yourdomain.com/docs
```

### Step 6: Setup SSL Auto-Renewal

```bash
# Enable certbot auto-renewal
sudo systemctl enable certbot.timer
sudo systemctl start certbot.timer

# Test renewal
sudo certbot renew --dry-run
```

## 🔒 Production Best Practices

### Security
- ✅ HTTPS enabled
- ✅ Strong TLS ciphers (1.2/1.3)
- ✅ Security headers set
- ✅ App not directly exposed
- ✅ Reverse proxy in front

### Monitoring
- Watch nginx logs: `docker logs emby-trakt-nginx`
- Watch app logs: `docker logs emby-trakt-suite`
- Monitor resources: `docker stats`

### Backups
```bash
# Backup database
docker exec emby-trakt-postgres pg_dump -U embytrakt embytrakt > backup.sql

# Backup volumes
docker run --rm -v emby-trakt-suite_postgres-data:/data -v $(pwd):/backup alpine tar czf /backup/postgres-backup.tar.gz /data
```

### Updates
```bash
# Pull latest code
git pull

# Rebuild containers
docker-compose -f deployment/docker-compose.production.yml build

# Restart services
docker-compose -f deployment/docker-compose.production.yml down
docker-compose -f deployment/docker-compose.production.yml up -d
```

## 🆘 Troubleshooting

### Certificate Errors
```bash
# Check certificate status
sudo certbot certificates

# Verify cert validity
openssl x509 -in /etc/letsencrypt/live/yourdomain.com/fullchain.pem -text -noout

# Renew manually
sudo certbot renew --force-renewal
```

### Nginx Not Starting
```bash
# Check config
docker run --rm -v $(pwd)/deployment/nginx.conf:/etc/nginx/conf.d/default.conf:ro nginx:alpine nginx -t

# Check logs
docker logs emby-trakt-nginx
```

### Port 80/443 In Use
```bash
# Find what's using the port
sudo lsof -i :80
sudo lsof -i :443

# If needed, kill process or change ports in docker-compose
```

### SSL Certificate Path Issues
Verify `/etc/letsencrypt` is accessible:
```bash
ls -la /etc/letsencrypt/live/yourdomain.com/
```

Should show:
```
fullchain.pem -> ../../../archive/yourdomain.com/fullchain1.pem
privkey.pem -> ../../../archive/yourdomain.com/privkey1.pem
```

## 📊 Performance Tuning

### Increase Worker Processes (if needed)
Edit `deployment/nginx.conf`:
```nginx
worker_processes 4;  # Match number of CPU cores
```

### Increase SSL Session Cache
```nginx
ssl_session_cache shared:SSL:50m;
ssl_session_timeout 1d;
```

### Enable Compression for More Types
```nginx
gzip_types text/plain text/css application/json application/javascript text/xml application/xml application/xml+rss;
```

## 🎯 Deployment Checklist

Pre-Deployment:
- [ ] Domain configured
- [ ] SSL certificate obtained
- [ ] nginx.conf updated
- [ ] .env file created
- [ ] All secrets generated
- [ ] Ports 80/443 available

Post-Deployment:
- [ ] HTTPS working
- [ ] HTTP redirects to HTTPS
- [ ] All services healthy
- [ ] API accessible
- [ ] WebSocket working
- [ ] SSL certificate valid
- [ ] Auto-renewal scheduled

## 📚 Additional Resources

- [Certbot Documentation](https://certbot.eff.org/)
- [Nginx Documentation](https://nginx.org/en/docs/)
- [Docker Compose Production Guide](https://docs.docker.com/compose/production/)
- [Let's Encrypt Documentation](https://letsencrypt.org/docs/)

---

**Need help?** Check `deployment/README.md` for more information about the deployment setup.
