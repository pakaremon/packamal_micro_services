from django.contrib import admin
from .models import Package, ReportDynamicAnalysis, APIKey, AnalysisTask
# Register your models here.

class PackageAdmin(admin.ModelAdmin):
    list_display = ('package_name', 'package_version', 'ecosystem')

class ReportDynamicAnalysisAdmin(admin.ModelAdmin):
    list_display = ('package', 'time', 'report')

class APIKeyAdmin(admin.ModelAdmin):
    list_display = ('name', 'key', 'is_active', 'rate_limit_per_hour', 'created_at', 'last_used')
    list_filter = ('is_active', 'created_at')
    search_fields = ('name', 'key')
    readonly_fields = ('key', 'created_at', 'last_used')

class AnalysisTaskAdmin(admin.ModelAdmin):
    list_display = ('purl', 'status', 'api_key', 'created_at', 'completed_at')
    list_filter = ('status', 'ecosystem', 'created_at')
    search_fields = ('purl', 'package_name', 'api_key__name')
    readonly_fields = ('created_at', 'started_at', 'completed_at')

admin.site.register(Package, PackageAdmin)
admin.site.register(ReportDynamicAnalysis, ReportDynamicAnalysisAdmin)
admin.site.register(APIKey, APIKeyAdmin)
admin.site.register(AnalysisTask, AnalysisTaskAdmin)

