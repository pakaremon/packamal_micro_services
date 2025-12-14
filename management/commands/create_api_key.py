"""
Django management command to create API keys
"""
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from package_analysis.models import APIKey


class Command(BaseCommand):
    help = 'Create a new API key for package analysis API access'

    def add_arguments(self, parser):
        parser.add_argument(
            '--name',
            type=str,
            required=True,
            help='Human-readable name for the API key'
        )
        parser.add_argument(
            '--rate-limit',
            type=int,
            default=100,
            help='Rate limit per hour (default: 100)'
        )
        parser.add_argument(
            '--inactive',
            action='store_true',
            help='Create the API key in inactive state'
        )

    def handle(self, *args, **options):
        name = options['name']
        rate_limit = options['rate_limit']
        is_active = not options['inactive']

        try:
            with transaction.atomic():
                api_key = APIKey.objects.create(
                    name=name,
                    rate_limit_per_hour=rate_limit,
                    is_active=is_active
                )
                
                self.stdout.write(
                    self.style.SUCCESS(f'Successfully created API key: {name}')
                )
                self.stdout.write(f'API Key: {api_key.key}')
                self.stdout.write(f'Rate Limit: {rate_limit} requests/hour')
                self.stdout.write(f'Status: {"Active" if is_active else "Inactive"}')
                self.stdout.write(f'Created: {api_key.created_at}')
                
                self.stdout.write(
                    self.style.WARNING(
                        '\nIMPORTANT: Store this API key securely. '
                        'It cannot be retrieved again once created.'
                    )
                )
                
        except Exception as e:
            raise CommandError(f'Failed to create API key: {str(e)}')



