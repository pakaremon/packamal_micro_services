from django.core.management.base import BaseCommand
from package_analysis.models import APIKey


class Command(BaseCommand):
    help = 'Create an API key'

    def add_arguments(self, parser):
        parser.add_argument('--name', required=True, help='Name for the API key owner')
        parser.add_argument('--rate', type=int, default=100, help='Requests per hour')

    def handle(self, *args, **options):
        name = options['name']
        rate = options['rate']
        api_key = APIKey.objects.create(name=name, rate_limit_per_hour=rate)
        self.stdout.write(self.style.SUCCESS(f"Created API key: {api_key.key}"))


