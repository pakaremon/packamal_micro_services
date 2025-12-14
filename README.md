# Pack-A-Mal Development Environment

Docker Compose-based development environment for Pack-A-Mal with separate services for backend, frontend, and database.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Nginx (Port 8080)                    │
│            Static Files Serving + Reverse Proxy         │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ↓
┌─────────────────────────────────────────────────────────┐
│              Django Backend (Port 8001)                  │
│              Pack-A-Mal Web Application                  │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ↓
┌─────────────────────────────────────────────────────────┐
│            PostgreSQL Database (Port 5433)              │
│                   packamal_dev                          │
└─────────────────────────────────────────────────────────┘
```

## Services

### 🐳 Backend (Django)
- **Container**: `packamal-backend-dev`
- **Port**: 8001 (mapped from 8000)
- **Framework**: Django 5.1.6
- **Python**: 3.12
- **Features**: Hot reload via volume mounting

### 🌐 Frontend (Nginx)
- **Container**: `packamal-frontend-dev`
- **Port**: 8080 (mapped from 80)
- **Purpose**: Serves static files and proxies to backend
- **Features**: Gzip compression, caching headers

### 💾 Database (PostgreSQL)
- **Container**: `packamal-db-dev`
- **Port**: 5433 (mapped from 5432)
- **Version**: PostgreSQL 15 Alpine
- **Database**: `packamal_dev`

## Quick Start

### Prerequisites
- Docker Desktop installed and running
- Docker Compose V2

### First Time Setup

```bash
cd dev

# Build all images
docker-compose build

# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Access:
# - Frontend: http://localhost:8080
# - Backend API: http://localhost:8001
# - Database: localhost:5433
```

### Using Makefile (Recommended)

```bash
# Start services
make up

# View logs
make logs

# Run migrations
make migrate

# Create superuser
make createsuperuser

# Access backend shell
make shell-backend

# Access database shell
make shell-db

# Stop services
make down

# See all commands
make help
```

## Configuration

### Environment Variables

Copy `.env.example` to `.env` and customize:

```bash
cp .env.example .env
```

Default values:
- **POSTGRES_DB**: packamal_dev
- **POSTGRES_USER**: pakaremon
- **POSTGRES_PASSWORD**: dev_password
- **DEBUG**: True
- **BACKEND_PORT**: 8001
- **FRONTEND_PORT**: 8080
- **DB_PORT**: 5433

## Scaling

### Scale Backend Service

```bash
# Scale to 3 instances
docker-compose up -d --scale backend=3

# Or using Makefile
make scale-backend

# View running instances
docker-compose ps
```

Nginx automatically load balances between backend instances.

## Data Persistence

All data persists in `./volumes/`:
- `postgres_data/` - Database files
- `media/` - Uploaded media files
- `static/` - Collected static files

## Development Workflow

### Making Code Changes

1. Edit files in `backend/` directory
2. Changes auto-reload (Django development server watches for changes)
3. Refresh browser to see updates

### Database Migrations

```bash
# Create migrations
docker-compose exec backend python manage.py makemigrations

# Apply migrations
docker-compose exec backend python manage.py migrate

# Or use Makefile
make migrate
```

### Installing New Dependencies

```bash
# Add to backend/requirements.txt
echo "new-package==1.0.0" >> backend/requirements.txt

# Rebuild backend image
docker-compose build backend

# Restart backend
docker-compose restart backend
```

## Useful Commands

### View Service Status
```bash
docker-compose ps
```

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f backend
docker-compose logs -f frontend
docker-compose logs -f database
```

#### Gunicorn Worker Identification

The backend uses Gunicorn with 4 workers, and each request log includes the worker's Process ID (PID) to identify which worker handled the request.

**Log Format:**
```
[Worker PID:xxxxx] IP - - [timestamp] "METHOD /path HTTP/1.1" STATUS SIZE "REFERER" "USER_AGENT" TIME_MS
```

**Example:**
```
[Worker PID:42] 172.18.0.1 - - [25/Dec/2024:10:30:45 +0000] "GET /api/endpoint HTTP/1.1" 200 1234 "-" "Mozilla/5.0" 12345
```

**How to track worker requests:**
```bash
# View backend logs with worker PIDs
docker-compose logs -f backend

# Filter logs by specific worker PID (e.g., worker with PID 42)
docker-compose logs -f backend | grep "PID:42"

# Count requests per worker
docker-compose logs backend | grep -o "PID:[0-9]*" | sort | uniq -c
```

**Note:** Each worker process has a unique PID. The PID appears at the start of each access log line, making it easy to trace which worker processed each request.

### Execute Commands in Containers
```bash
# Django shell
docker-compose exec backend python manage.py shell

# Database shell
docker-compose exec database psql -U pakaremon -d packamal_dev

# Bash shell in backend
docker-compose exec backend bash
```

### Collect Static Files
```bash
docker-compose exec backend python manage.py collectstatic --noinput
```

### Run Tests
```bash
docker-compose exec backend python manage.py test
```

## Troubleshooting

### Container Won't Start

```bash
# Check logs
docker-compose logs backend

# Rebuild image
docker-compose build --no-cache backend
docker-compose up -d
```

### Database Connection Issues

```bash
# Check database is healthy
docker-compose exec database pg_isready -U pakaremon

# Verify environment variables
docker-compose exec backend env | grep POSTGRES
```

### Port Already in Use

Change ports in `docker-compose.yml`:
- Backend: `"8001:8000"` → `"9001:8000"`
- Frontend: `"8080:80"` → `"9080:80"`
- Database: `"5433:5432"` → `"5434:5432"`

### Clean Start

```bash
# Remove all containers and volumes
docker-compose down -v

# Rebuild and start fresh
docker-compose build
docker-compose up -d
```

## Production vs Development

| Aspect | Development (this env) | Production |
|--------|----------------------|------------|
| DEBUG | True | False |
| Database | Local PostgreSQL container | External PostgreSQL server |
| Static Files | Served by Nginx from volume | CDN or dedicated server |
| WSGI Server | Django runserver | Gunicorn/uWSGI |
| Ports | 8001, 8080, 5433 | 80, 443 |
| Secrets | Hardcoded in .env | Environment variables/secrets manager |

## Next Steps

1. **Create Superuser**: `make createsuperuser`
2. **Access Admin**: http://localhost:8080/admin
3. **Run Tests**: `make test`
4. **Start Development**: Edit files in `backend/`

## Structure

```
dev/
├── docker-compose.yml       # Service orchestration
├── Makefile                # Helper commands
├── .env.example            # Environment template
├── backend/                # Django application
│   ├── Dockerfile
│   ├── entrypoint.sh
│   ├── requirements.txt
│   ├── manage.py
│   ├── packamal/           # Django project
│   └── package_analysis/   # Django app
├── frontend/               # Nginx reverse proxy
│   ├── Dockerfile
│   └── nginx.conf
├── database/              # PostgreSQL initialization
│   └── init.sql
└── volumes/               # Persistent data
    ├── postgres_data/
    ├── media/
    └── static/
```

## Support

- Project Docs: See `comprehensive_documentation.md` in parent directory
- Django Docs: https://docs.djangoproject.com/
- Docker Compose Docs: https://docs.docker.com/compose/


## restart
# Stop everything
docker-compose down

# Remove the local Postgres data directory used by the bind mount
rm -rf ./volumes/postgres_data

# Bring services back up (this will let Postgres fully re-init)
docker-compose up -d

## using volumns to save dynamic analyis results

Quick ways to inspect/read:
1) List files in a volume
docker run --rm -v analysis_results:/data alpine ls -lah /data
2) View a file
docker run --rm -v analysis_results:/data alpine cat /data/path/to/file.json
3) Copy files to your host (current directory)
docker run --rm -v analysis_results:/data -v "$PWD":/host alpine sh -c 'cp -r /data/* /host/'
4) Inspect what path a volume uses on the host (not always needed)
docker volume inspect analysis_results
If you want to browse interactively:
docker run --rm -it -v analysis_results:/data alpine sh# then inside: ls /data