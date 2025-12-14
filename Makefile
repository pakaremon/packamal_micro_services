.PHONY: help build up down restart logs shell-backend shell-db migrate collectstatic createsuperuser test clean scale-backend

help:
	@echo "Pack-A-Mal Development Environment - Available Commands:"
	@echo ""
	@echo "  make build           - Build all Docker images"
	@echo "  make up              - Start all services"
	@echo "  make down            - Stop all services"
	@echo "  make restart         - Restart all services"
	@echo "  make logs            - View logs from all services"
	@echo "  make shell-backend   - Open bash shell in backend container"
	@echo "  make shell-db        - Open PostgreSQL shell"
	@echo "  make migrate         - Run Django migrations"
	@echo "  make collectstatic   - Collect static files"
	@echo "  make createsuperuser - Create Django superuser"
	@echo "  make test            - Run tests"
	@echo "  make clean           - Remove all containers and volumes"
	@echo "  make scale-backend   - Scale backend to 3 instances"
	@echo ""

build:
	docker-compose build

up:
	docker-compose up -d
	@echo ""
	@echo "Services started successfully!"
	@echo "Backend: http://localhost:8001"
	@echo "Frontend (Nginx): http://localhost:8080"
	@echo "Database: localhost:5433"
	@echo ""

down:
	docker-compose down

restart:
	docker-compose restart

logs:
	docker-compose logs -f

shell-backend:
	docker-compose exec backend bash

shell-db:
	docker-compose exec database psql -U pakaremon -d packamal_dev

migrate:
	docker-compose exec backend python manage.py migrate

collectstatic:
	docker-compose exec backend python manage.py collectstatic --noinput

createsuperuser:
	docker-compose exec backend python manage.py createsuperuser

test:
	docker-compose exec backend python manage.py test

clean:
	docker-compose down -v
	@echo "All containers and volumes removed!"

scale-backend:
	docker-compose up -d --scale backend=3
	@echo ""
	@echo "Backend scaled to 3 instances"
	@docker-compose ps
