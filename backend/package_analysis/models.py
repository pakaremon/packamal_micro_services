from django.db import models
from django.db.models import Q
from django.utils import timezone
import secrets
import string

# Create your models here.

class Package(models.Model):
    package_name = models.CharField(max_length=20)
    package_version = models.CharField(max_length=20)
    ecosystem = models.CharField(max_length=20)

    def __str__(self):
        return f"{self.package_name} {self.package_version} {self.ecosystem}"
    

    

class ReportTyposquatting(models.Model):
    # report mutiple tools: typosquatting, and source code finder.
    package = models.OneToOneField(Package, on_delete=models.CASCADE, related_name='report_typosquatting')
    typosquatting_candidates = models.JSONField(default=list)  # Provide a default value


class ReportSource(models.Model):
    # report mutiple tools: typosquatting, and source code finder.
    package = models.OneToOneField(Package, on_delete=models.CASCADE, related_name='report_source')
    source_url = models.JSONField(default=list)  # Provide a default value

class ReportDynamicAnalysis(models.Model):
    # report dynamic analysis
    package = models.OneToOneField(Package, on_delete=models.CASCADE, related_name='report_dynamic_analysis')
    time = models.FloatField()
    report = models.JSONField(default=dict)  # Provide a default value
    

    def __str__(self):
        return f"{self.package} {self.time} (seconds)"


class APIKey(models.Model):
    """Model for API key authentication"""
    name = models.CharField(max_length=100, help_text="Human-readable name for this API key")
    key = models.CharField(max_length=64, unique=True, help_text="The actual API key")
    created_at = models.DateTimeField(auto_now_add=True)
    is_active = models.BooleanField(default=True)
    rate_limit_per_hour = models.IntegerField(default=100, help_text="Maximum requests per hour")
    last_used = models.DateTimeField(null=True, blank=True)
    
    def save(self, *args, **kwargs):
        if not self.key:
            # Generate a secure random API key
            self.key = self.generate_api_key()
        super().save(*args, **kwargs)
    
    @staticmethod
    def generate_api_key():
        """Generate a secure random API key"""
        alphabet = string.ascii_letters + string.digits
        return ''.join(secrets.choice(alphabet) for _ in range(64))
    
    def __str__(self):
        return f"{self.name} ({self.key[:8]}...)"


class AnalysisTask(models.Model):
    """Model to track API analysis requests"""
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('queued', 'Queued'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]
    
    api_key = models.ForeignKey(APIKey, on_delete=models.CASCADE, related_name='analysis_tasks')
    purl = models.CharField(max_length=500, help_text="Package URL submitted for analysis")
    package_name = models.CharField(max_length=200, blank=True)
    package_version = models.CharField(max_length=100, blank=True)
    ecosystem = models.CharField(max_length=50, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    created_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    error_message = models.TextField(blank=True)
    error_category = models.CharField(max_length=50, blank=True, help_text="Category of error (e.g., docker_error, timeout_error, analysis_error)")
    error_details = models.JSONField(default=dict, blank=True, help_text="Detailed error information including stderr, stdout, etc.")
    report = models.OneToOneField(ReportDynamicAnalysis, on_delete=models.SET_NULL, null=True, blank=True)
    download_url = models.URLField(blank=True, null=True, help_text="URL to download the analysis report JSON file")
    idempotency_key = models.CharField(max_length=64, blank=True, null=True, db_index=True)
    
    # Queue management fields
    queue_position = models.PositiveIntegerField(null=True, blank=True, help_text="Position in the analysis queue")
    priority = models.PositiveIntegerField(default=0, help_text="Priority level (higher number = higher priority)")
    queued_at = models.DateTimeField(null=True, blank=True, help_text="When the task was added to the queue")
    
    # Timeout management fields
    timeout_minutes = models.PositiveIntegerField(default=30, help_text="Timeout in minutes for this task")
    container_id = models.CharField(max_length=100, blank=True, null=True, help_text="Docker container ID if running")
    last_heartbeat = models.DateTimeField(null=True, blank=True, help_text="Last heartbeat from running container")
    
    def is_timed_out(self):
        """Check if this task has exceeded its timeout."""
        if self.status != 'running' or not self.started_at:
            return False
        
        from django.utils import timezone
        timeout_delta = timezone.timedelta(minutes=self.timeout_minutes)
        return timezone.now() > (self.started_at + timeout_delta)
    
    def get_remaining_time_minutes(self):
        """Get remaining time in minutes before timeout."""
        if self.status != 'running' or not self.started_at:
            return None
        
        from django.utils import timezone
        timeout_delta = timezone.timedelta(minutes=self.timeout_minutes)
        remaining = (self.started_at + timeout_delta) - timezone.now()
        return max(0, int(remaining.total_seconds() / 60))
    
    def __str__(self):
        return f"AnalysisTask: {self.purl} ({self.status})"
    
    class Meta:
        indexes = [
            models.Index(fields=['purl']),
            models.Index(fields=['status', 'created_at']),
            models.Index(fields=['api_key', 'created_at']),
            models.Index(fields=['status', 'queue_position']),
            models.Index(fields=['priority', 'queued_at']),
            models.Index(fields=['status', 'started_at']),
            models.Index(fields=['container_id']),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=['api_key', 'idempotency_key'],
                name='unique_api_key_idempotency_key',
                condition=Q(idempotency_key__isnull=False),
            )
        ]
