from django.core.management.base import BaseCommand
from django.conf import settings
from django.db import connection
import os
import sys
import traceback


class Command(BaseCommand):
    help = 'Display comprehensive debugging information about the Django project'

    def add_arguments(self, parser):
        parser.add_argument(
            '--check-db',
            action='store_true',
            help='Check database connectivity',
        )
        parser.add_argument(
            '--check-settings',
            action='store_true',
            help='Display current settings',
        )
        parser.add_argument(
            '--check-urls',
            action='store_true',
            help='Display URL patterns',
        )

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('=== Django Debug Information ==='))
        
        # Basic project info
        self.stdout.write(f'Python version: {sys.version}')
        self.stdout.write(f'Django version: {settings.VERSION}')
        self.stdout.write(f'Project root: {settings.BASE_DIR}')
        self.stdout.write(f'Debug mode: {settings.DEBUG}')
        self.stdout.write(f'Allowed hosts: {settings.ALLOWED_HOSTS}')
        
        # Check database
        if options['check_db']:
            self.check_database()
        
        # Check settings
        if options['check_settings']:
            self.check_settings()
        
        # Check URLs
        if options['check_urls']:
            self.check_urls()
        
        # Always show some basic info
        self.show_installed_apps()
        self.show_middleware()
        self.show_static_files_config()

    def check_database(self):
        self.stdout.write('\n=== Database Information ===')
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT sqlite_version();")
                version = cursor.fetchone()
                self.stdout.write(f'SQLite version: {version[0]}')
                
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                self.stdout.write(f'Database tables: {[table[0] for table in tables]}')
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Database error: {e}'))

    def check_settings(self):
        self.stdout.write('\n=== Settings Information ===')
        self.stdout.write(f'Secret key length: {len(settings.SECRET_KEY)}')
        self.stdout.write(f'Database engine: {settings.DATABASES["default"]["ENGINE"]}')
        self.stdout.write(f'Database name: {settings.DATABASES["default"]["NAME"]}')
        self.stdout.write(f'Static URL: {settings.STATIC_URL}')
        self.stdout.write(f'Media URL: {settings.MEDIA_URL}')

    def check_urls(self):
        self.stdout.write('\n=== URL Patterns ===')
        try:
            from django.urls import get_resolver
            resolver = get_resolver()
            self.display_url_patterns(resolver.url_patterns, '')
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error displaying URLs: {e}'))

    def display_url_patterns(self, patterns, prefix=''):
        for pattern in patterns:
            if hasattr(pattern, 'url_patterns'):
                self.stdout.write(f'{prefix}├── {pattern.pattern} (include)')
                self.display_url_patterns(pattern.url_patterns, prefix + '│   ')
            else:
                self.stdout.write(f'{prefix}└── {pattern.pattern} -> {pattern.callback.__name__ if hasattr(pattern, "callback") else "unknown"}')

    def show_installed_apps(self):
        self.stdout.write('\n=== Installed Apps ===')
        for app in settings.INSTALLED_APPS:
            self.stdout.write(f'  - {app}')

    def show_middleware(self):
        self.stdout.write('\n=== Middleware ===')
        for middleware in settings.MIDDLEWARE:
            self.stdout.write(f'  - {middleware}')

    def show_static_files_config(self):
        self.stdout.write('\n=== Static Files Configuration ===')
        self.stdout.write(f'Static URL: {settings.STATIC_URL}')
        self.stdout.write(f'Static root: {settings.STATIC_ROOT}')
        self.stdout.write(f'Static dirs: {settings.STATICFILES_DIRS}')
        self.stdout.write(f'Media URL: {settings.MEDIA_URL}')
        self.stdout.write(f'Media root: {settings.MEDIA_ROOT}') 