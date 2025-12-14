from django.shortcuts import render, redirect
from django.http import HttpResponse, JsonResponse, HttpResponseNotFound
from django.views.decorators.csrf import csrf_exempt

from .forms import PackageSubmitForm

from .helper import Helper
import json
import logging
import traceback

from django.core.files.storage import FileSystemStorage

from .models import Package, ReportDynamicAnalysis, APIKey, AnalysisTask
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from .src.py2src.py2src.url_finder import   URLFinder
from .utils import PURLParser, validate_purl_format
from .api_utils import json_success, json_error, api_handler
from .auth import require_api_key
from django.utils import timezone
from django.urls import reverse
from django.conf import settings
from .redis_client import get_redis_client
# QueueManager removed - using Celery instead
# from .queue_manager import queue_manager

logger = logging.getLogger(__name__)


def _safe_package_name(raw_name: str) -> str:
    """Normalize package name for storage keys and URLs."""
    return raw_name.replace('/', '_').replace('\\', '_')


def save_professional_report(completed_task, request):
    """
    Save analysis report into Redis with key ecosystem:package:version.
    Returns download URL and report metadata.
    """
    from datetime import datetime

    # Extract the report data from the JSONField
    report_data = completed_task.report
    if hasattr(report_data, 'report'):
        # If it's a ReportDynamicAnalysis object, get the report field
        report_json = report_data.report
    else:
        # If it's already the report data
        report_json = report_data

    ecosystem = completed_task.ecosystem.lower()
    package_name = _safe_package_name(completed_task.package_name)
    version = completed_task.package_version
    now = datetime.now()

    redis_client = get_redis_client()
    redis_key = f"ecosystem:{ecosystem}:{package_name}:{version}"
    ttl_seconds = getattr(settings, "PROFESSIONAL_REPORT_TTL_SECONDS", 24 * 60 * 60)

    # If already in Redis, reuse it to avoid extra work
    existing_blob = redis_client.get(redis_key)
    if existing_blob:
        download_path = f"/reports/{ecosystem}/{package_name}/{version}.json"
        download_url = request.build_absolute_uri(download_path)

        try:
            existing_json = json.loads(existing_blob)
            created_at = existing_json.get("metadata", {}).get("created_at")
        except Exception:
            created_at = None

        ttl_remaining = redis_client.ttl(redis_key)
        expires_in = ttl_remaining if ttl_remaining and ttl_remaining > 0 else ttl_seconds

        report_metadata = {
            "filename": f"{version}.json",
            "size_bytes": len(existing_blob),
            "created_at": created_at,
            "download_url": download_url,
            "folder_structure": f"reports/{ecosystem}/{package_name}/",
            "redis_key": redis_key,
            "expires_in_seconds": expires_in,
        }

        return download_url, report_metadata

    # Build enhanced payload with metadata when not cached
    enhanced_report = {
        "metadata": {
            "created_at": now.isoformat(),
            "package": {
                "name": completed_task.package_name,
                "version": version,
                "ecosystem": completed_task.ecosystem,
                "purl": completed_task.purl,
            },
            "analysis": {
                "status": "completed",
                "started_at": completed_task.started_at.isoformat() if completed_task.started_at else None,
                "completed_at": completed_task.completed_at.isoformat() if completed_task.completed_at else None,
                "duration_seconds": completed_task.report.time if hasattr(completed_task.report, 'time') else None,
            },
            "api": {
                "version": "1.0",
                "endpoint": "analyze_api",
                "generated_by": "Pack-a-mal Analysis Platform",
            },
        },
        "analysis_results": report_json,
    }

    json_payload = json.dumps(enhanced_report, ensure_ascii=False)
    redis_client.setex(redis_key, ttl_seconds, json_payload)

    # Build download URL served via Django (proxied by nginx)
    download_path = f"/reports/{ecosystem}/{package_name}/{version}.json"
    download_url = request.build_absolute_uri(download_path)

    report_metadata = {
        "filename": f"{version}.json",
        "size_bytes": len(json_payload.encode("utf-8")),
        "created_at": now.isoformat(),
        "download_url": download_url,
        "folder_structure": f"reports/{ecosystem}/{package_name}/",
        "redis_key": redis_key,
        "expires_in_seconds": ttl_seconds,
    }

    return download_url, report_metadata


def download_professional_report(request, ecosystem, package_name, package_version):
    """
    Serve professional report JSON directly from Redis by key.
    """
    redis_client = get_redis_client()
    safe_package = _safe_package_name(package_name)
    redis_key = f"ecosystem:{ecosystem.lower()}:{safe_package}:{package_version}"
    report_blob = redis_client.get(redis_key)

    if not report_blob:
        return HttpResponseNotFound("Report not found or expired")

    return HttpResponse(report_blob, content_type="application/json")


def save_report(reports):

    package, created = Package.objects.get_or_create(
        package_name=reports['packages']['package_name'],
        package_version=reports['packages']['package_version'],
        ecosystem=reports['packages']['ecosystem']
    )
    report = ReportDynamicAnalysis.objects.create(
        package=package,
        time=reports['time'],
        report=reports
    )
    report.save()



def dashboard(request):
    form = PackageSubmitForm()
    return render(request, 'package_analysis/dashboard.html', {'form': form})

def contact(request):
    return render(request, 'package_analysis/homepage/contact.html')
 
def homepage(request):
    return render(request, 'package_analysis/homepage/homepage.html')

def dynamic_analysis(request):
    """
    Dynamic analysis endpoint - ASYNC with Celery
    """
    if request.method == 'POST':
        print("🚀 Dynamic analysis POST request")
        form = PackageSubmitForm(request.POST)
        if form.is_valid():
            try:
                package_name = form.cleaned_data['package_name']
                package_version = form.cleaned_data['package_version']
                ecosystem = form.cleaned_data['ecosystem']
                
                print(f"📦 Package: {package_name}@{package_version} ({ecosystem})")
                
                # Create task record
                from django.db import transaction
                with transaction.atomic():
                    task = AnalysisTask.objects.create(
                        package_name=package_name,
                        package_version=package_version,
                        ecosystem=ecosystem,
                        status='queued',
                        queued_at=timezone.now()
                    )
                    
                    # Calculate queue position
                    queued_count = AnalysisTask.objects.filter(
                        status='queued'
                    ).exclude(id=task.id).count()
                    task.queue_position = queued_count + 1
                    task.save()
                
                # Queue Celery task with priority
                from .tasks import run_dynamic_analysis
                celery_task = run_dynamic_analysis.apply_async(
                    args=[task.id],
                    priority=task.priority,
                    queue='analysis'
                )
                
                print(f"Queued task {task.id} (Celery ID: {celery_task.id})")
                
                return JsonResponse({
                    "status": "pending",
                    "task_id": task.id,
                    "message": "Analysis queued successfully"
                })
            except Exception as e:
                print(f"Error: {str(e)}")
                return JsonResponse({
                    "status": "error",
                    "error": str(e)
                }, status=500)
    
    form = PackageSubmitForm()
    return render(request, 'package_analysis/analysis/dynamic_analysis.html', {'form': form}) 

def malcontent(request):
    if request.method == 'POST':
        form = PackageSubmitForm(request.POST)
        if form.is_valid():
            package_name = form.cleaned_data['package_name']
            package_version = form.cleaned_data['package_version']
            ecosystem = form.cleaned_data['ecosystem']
            
            reports = Helper.run_malcontent(package_name, package_version, ecosystem)
            return JsonResponse({"malcontent_report": reports})
    form = PackageSubmitForm()
    return render(request, 'package_analysis/analysis/malcontent.html', {'form': form})

def lastpymile(request):
    if request.method == 'POST':
        print("lastpymile Post ^^^^")
        form = PackageSubmitForm(request.POST)
        if form.is_valid():
            print("lastpymile form is valid")
            package_name = form.cleaned_data['package_name']
            package_version = form.cleaned_data['package_version']
            ecosystem = form.cleaned_data['ecosystem']

            # Process the form data (e.g., save to database, call an API, etc.)
            print(f"Package Name: {package_name}, Package Version: {package_version}, Ecosystem: {ecosystem}")
            reports = Helper.run_lastpymile(package_name, package_version, ecosystem)
            return JsonResponse({"lastpymile_report": reports})
    form = PackageSubmitForm()
    return render(request, 'package_analysis/analysis/lastpymile.html', {'form': form})

def bandit4mal(request):
    print("submit static analysis bandit4mal tools")
    if request.method == 'POST':
        form = PackageSubmitForm(request.POST)
        if form.is_valid():
            print("bandit4mal form is valid")
            package_name = form.cleaned_data['package_name']
            package_version = form.cleaned_data['package_version']
            ecosystem = form.cleaned_data['ecosystem']

            # Process the form data (e.g., save to database, call an API, etc.)
            print(f"Package Name: {package_name}, Package Version: {package_version}, Ecosystem: {ecosystem}")
            reports = Helper.run_bandit4mal(package_name, package_version, ecosystem)
            return JsonResponse({"bandit4mal_report": reports})
    form = PackageSubmitForm()
    return render(request, 'package_analysis/analysis/bandit4mal.html', {'form': form})

def find_typosquatting(request):
    print("find typosquatting")
    if request.method == 'POST':
        form = PackageSubmitForm(request.POST)
        if form.is_valid():
            package_name = form.cleaned_data['package_name']
            package_version = form.cleaned_data['package_version']
            ecosystem = form.cleaned_data['ecosystem']

            # Process the form data (e.g., save to database, call an API, etc.)
            print(f"find oss-squat:package Name: {package_name}, Package Version: {package_version}, Ecosystem: {ecosystem}")
            typo_candidates = Helper.run_oss_squats(package_name, package_version, ecosystem)
            print("Typo candidates: ", typo_candidates)
            return JsonResponse({'typosquatting_candidates': typo_candidates})
        
    form = PackageSubmitForm()
    return render(request, 'package_analysis/analysis/typosquatting.html', {'form': form})

def task_status(request, task_id):
    """
    API endpoint to check task status for async analysis
    Returns: task status, progress, and results when completed
    """
    try:
        task = AnalysisTask.objects.get(id=task_id)
        
        response_data = {
            'task_id': task.id,
            'status': task.status,  # queued, running, completed, failed
            'package_name': task.package_name,
            'package_version': task.package_version,
            'ecosystem': task.ecosystem,
            'created_at': task.created_at.isoformat() if task.created_at else None,
        }
        
        if task.started_at:
            response_data['started_at'] = task.started_at.isoformat()
        
        if task.completed_at:
            response_data['completed_at'] = task.completed_at.isoformat()
            if task.duration_seconds:
                response_data['duration_seconds'] = task.duration_seconds
        
        if task.worker_id:
            response_data['worker_id'] = task.worker_id
        
        # If completed, include results
        if task.status == 'completed' and task.result:
            response_data['dynamic_analysis_report'] = task.result
        
        # If failed, include error
        if task.status == 'failed':
            response_data['error_message'] = task.error_message if hasattr(task, 'error_message') else 'Unknown error'
        
        return JsonResponse(response_data)
        
    except AnalysisTask.DoesNotExist:
        return JsonResponse({
            'error': 'Task not found',
            'task_id': task_id
        }, status=404)


def find_source_code(request):
    if request.method == 'POST':
        print("find source code")
        form = PackageSubmitForm(request.POST)
        if form.is_valid():
            print("find source code form is valid")
            package_name = form.cleaned_data['package_name']
            package_version = form.cleaned_data['package_version']
            ecosystem = form.cleaned_data['ecosystem']

            # Process the form data (e.g., save to database, call an API, etc.)
            if ecosystem == "pypi":
                sources = Helper.run_py2src(package_name, package_version, ecosystem)
            else: 
                urls = Helper.run_oss_find_source(package_name, package_version, ecosystem)
                sources = []
                for url in urls:
                    if url != "" and URLFinder.test_url_working(URLFinder.normalize_url(url)):
                        sources.append(URLFinder.real_github_url(url))

                sources = list(set(sources))
        

            return JsonResponse({'source_urls': sources})
        
    form = PackageSubmitForm()
    return render(request, 'package_analysis/analysis/findsource.html', {'form': form})

def submit_sample(request):
    # TODO: if package has already been analyzed, return the report instead of re-analyzing it.

    ''' Enter package name, version and ecosystem to analyze the package.
      The package are already in the Wolfi registry'''
    if request.method == 'POST':
        form = PackageSubmitForm(request.POST)
        if form.is_valid():

            package_name = form.cleaned_data['package_name']
            package_version = form.cleaned_data['package_version']
            ecosystem = form.cleaned_data['ecosystem']
            # Process the form data (e.g., save to database, call an API, etc.)
            print(f"Package Name: {package_name}, Package Version: {package_version}, Ecosystem: {ecosystem}")

            with ThreadPoolExecutor() as executor:
                future_reports = executor.submit(Helper.run_package_analysis, package_name, package_version, ecosystem)
                future_typosquatting_candidates = executor.submit(Helper.run_oss_squats, package_name, package_version, ecosystem)
                future_sources = executor.submit(Helper.run_oss_find_source, package_name, package_version, ecosystem)

                reports = future_reports.result()
                typo_candidates = future_typosquatting_candidates.result()
                sources = future_sources.result()

                reports['sources'] = sources
                reports['typo_candidates'] = typo_candidates

                print("Typo candidates: ", reports['typo_candidates'])

            
            # save_report(reports)
            latest_report = ReportDynamicAnalysis.objects.latest('id')
            reports['id'] = latest_report.id
            return JsonResponse(reports)
    else:
        return JsonResponse({'status': 'error', 'message': 'Invalid request'}, status=400)


def upload_sample(request):
    ''' Upload sample  analysis it'''
    if request.method == 'POST' and request.FILES['file']:
         
        file = request.FILES['file']
        fs = FileSystemStorage()
        filename = fs.save(file.name, file)
        uploaded_file_url = fs.url(filename)
        ecosystem = request.POST.get('ecosystem', None)
        package_name = request.POST.get('package_name', None)
        package_version = request.POST.get('package_version', None)
        
        reports = Helper.handle_uploaded_file(uploaded_file_url, package_name, package_version, ecosystem)
        
        # Save to database
        # save_report(reports)
        # latest_report = Report.objects.latest('id')
        # reports['id'] = latest_report.id
        # delete the uploaded file
        fs.delete(filename)
        return JsonResponse({"dynamic_analysis_report": reports})
    else:
        return JsonResponse({'status': 'error', 'message': 'Invalid request'}, status=400)
    


def report_detail(request, report_id):
    '''Report detail analysis result of the package'''
    report = ReportDynamicAnalysis.objects.get(pk=report_id)
    return render(request, 'package_analysis/report_detail.html', {'report': report})

def get_all_report(request):
    report = ReportDynamicAnalysis.objects.all()
    results = {}
    for r in report:
        results[r.id] = {
            'id': r.id,
            'package_name': r.package.package_name,
            'package_version': r.package.package_version,
            'ecosystem': r.package.ecosystem,
            'time': r.time,
        }

    return JsonResponse(results)

def get_report(request, report_id):
    report = ReportDynamicAnalysis.objects.get(pk=report_id)
    results = {
        'package_name': report.package.package_name,
        'package_version': report.package.package_version,
        'ecosystem': report.package.ecosystem,
        'time': report.time,
        'report_data': report.report,
    }
    return JsonResponse(results)

def analyzed_samples(request):
    '''List of analyzed samples, sorted by id'''

    packages = Package.objects.all().order_by('-id')

    return render(request, 'package_analysis/analyzed_samples.html', {'packages': packages})

def get_wolfi_packages(request):
    return JsonResponse(Helper.get_wolfi_packages())

def get_maven_packages(request):
    return JsonResponse(Helper.get_maven_packages())

def get_rust_packages(request):
    return JsonResponse(Helper.get_rust_packages())

def get_pypi_packages(request):
    return JsonResponse(Helper.get_pypi_packages() )

def get_npm_packages(request):
    return JsonResponse(Helper.get_npm_packages())

@staticmethod
def get_packagist_packages(request):
    return JsonResponse(Helper.get_packagist_packages())

@staticmethod
def get_rubygems_packages(request):
    return JsonResponse(Helper.get_rubygems_packages())

@staticmethod
def get_rubygems_versions(request):
    import requests
    def get_package_versions(package_name):
        url = f"https://rubygems.org/api/v1/versions/{package_name}.json"

        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            results = []
            for version in data:
                results.append(version['number'])
            return results
        else:
            return []
        
    package_name = request.GET.get('package_name', None)
    if not package_name:
        return JsonResponse({'error': 'Package name is required'}, status=400)
    
    get_package_versions = get_package_versions(package_name)
    return JsonResponse({"versions": get_package_versions})

@staticmethod
def get_packagist_versions(request):
    import requests
    def get_package_versions(package_name):
        url = f"https://repo.packagist.org/p2/{package_name}.json"
        print("get_packagist_versions url: ", url)

        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            results = []
            for version in data['packages'].get(package_name, []):
                results.append(version['version'])
            return results
        else:
            return []  # Return an empty list if the request fails
    
    package_name = request.GET.get('package_name', None)
    if not package_name:
        return JsonResponse({'error': 'Package name is required'}, status=400)
    
    package_versions = get_package_versions(package_name)
    return JsonResponse({"versions": package_versions})



       
           



def get_npm_versions(request):
    import requests
    def get_package_versions(package_name):
        url = f'https://registry.npmjs.org/{package_name}'
        
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            versions = list(data.get('versions', {}).keys())
            latest_version = data.get('dist-tags', {}).get('latest')
            
            return versions
        else:
            print(f"Failed to fetch {package_name}: {response.status_code}")
            return None
        
    package_name = request.GET.get('package_name', None)
    if not package_name:
        return JsonResponse({'error': 'Package name is required'}, status=400)
    
    package_versions = get_package_versions(package_name)
    return JsonResponse({"versions": package_versions})

def get_pypi_versions(request):
    package_name = request.GET.get('package_name', None)
    if not package_name:
        return JsonResponse({'error': 'Package name is required'}, status=400)
    
    import requests
    def get_versions(package_name):
        """Get all available versions of a package from PyPI."""
        url = f"https://pypi.org/pypi/{package_name}/json"
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            versions = list(data['releases'].keys())  # Directly convert to list
            return versions
        else:
            return []  # Return empty list if request fails
         
    # Get the versions of the package
    versions = get_versions(package_name)

    return JsonResponse({"versions":versions})


def get_predicted_download_url(request, package_name, package_version, ecosystem):
    '''
    Get the predicted download URL for the final JSON report
    Args:
        request: the request object
        package_name: the name of the package
        package_version: the version of the package
        ecosystem: the ecosystem of the package
    Returns:
        the predicted download URL

    
    '''

    ecosystem_lower = ecosystem.lower()
    sanitized_package_name = _safe_package_name(package_name)
    return request.build_absolute_uri(f"/reports/{ecosystem_lower}/{sanitized_package_name}/{package_version}.json")


@csrf_exempt
@require_api_key
@api_handler
def analyze_api(request):
    """
    API endpoint to analyze packages via PURL
    Accepts POST requests with PURL in JSON body
    Returns analysis task ID and result URL
    Uses queue system to ensure only one container runs at a time
    """
    if request.method != 'POST':
        return json_error(request, error='Method not allowed', message='Only POST requests are supported', status=405)
    
    try:
        # Parse JSON request body
        data = json.loads(request.body)
        purl = data.get('purl')
        priority = data.get('priority', 0)  # Allow priority to be specified
        
        if not purl:
            return json_error(request, error='Missing PURL', message='PURL parameter is required', status=400)
        
        # Validate PURL format
        if not validate_purl_format(purl):
            return json_error(request, error='Invalid PURL format', message='PURL must be a valid package URL starting with pkg:', status=400)
        
        # Parse PURL to extract package information
        try:
            package_name, package_version, ecosystem = PURLParser.extract_package_info(purl)
        except ValueError as e:
            return json_error(request, error='PURL parsing failed', message=str(e), status=400)
        
        # First, check for ANY completed task for this PURL (regardless of time)
        # This ensures we always return cached results if they exist
        completed_task = AnalysisTask.objects.filter(
            purl=purl,
            status='completed',
            report__isnull=False
        ).order_by('-completed_at').first()
        
        if completed_task:
            print(f"DEBUG: Found completed task {completed_task.id} for PURL: {purl}")
            
            # Return existing analysis result
            result_url = request.build_absolute_uri(
                reverse('get_report', args=[completed_task.report.id])
            )

            
            # Save the completed analysis report as a downloadable JSON file on the server
            download_url, report_metadata = save_professional_report(completed_task, request)

            return JsonResponse({
                'task_id': completed_task.id,
                'status': 'completed',
                'result_url': download_url,
                'report_metadata': report_metadata,
                'message': 'Analysis already exists (cached result)'
            })
        
        # Check for existing active tasks (running, queued, or pending) within the last 24 hours
        existing_active_tasks = AnalysisTask.objects.filter(
            purl=purl,
            status__in=['running', 'queued', 'pending'],
            created_at__gte=timezone.now() - timezone.timedelta(hours=24)
        ).order_by('-created_at')
        
        # Debug: Print existing active tasks for troubleshooting
        print(f"DEBUG: Found {existing_active_tasks.count()} existing active tasks for PURL: {purl}")
        for task in existing_active_tasks:
            print(f"  Task {task.id}: status={task.status}, created={task.created_at}")
        
        # Check for running, queued, or pending task
        active_task = existing_active_tasks.first()
        if active_task:
            # Build predicted download URL for the final JSON report
            predicted_download_url = get_predicted_download_url(request, package_name, package_version, ecosystem)

            status_url = request.build_absolute_uri(
                reverse('task_status_api', args=[active_task.id])
            )

            # Get queue position if task is queued
            queue_position = None
            if active_task.status == 'queued':
                queue_position = active_task.queue_position

            return json_success(request, {
                'task_id': active_task.id,
                'status': active_task.status,
                'status_url': status_url,
                'result_url': predicted_download_url,
                'queue_position': queue_position,
                'message': f'Analysis already {active_task.status}'
            })
        
        # Final check before creating task (prevent race conditions)
        last_check = existing_active_tasks.filter(
            created_at__gte=timezone.now() - timezone.timedelta(minutes=1)
        ).first()
        
        if last_check:
            queue_position = None
            if last_check.status == 'queued':
                queue_position = last_check.queue_position
                
            return json_success(request, {
                'task_id': last_check.id,
                'status': last_check.status,
                'queue_position': queue_position,
                'message': f'Analysis already {last_check.status} (race condition prevented)'
            })
        
        # Idempotency: allow client to pass X-Idempotency-Key header to dedupe
        idempotency_key = request.META.get('HTTP_X_IDEMPOTENCY_KEY')

        # Create new analysis task
        task_defaults = dict(
            api_key=request.api_key,
            purl=purl,
            package_name=package_name,
            package_version=package_version,
            ecosystem=ecosystem,
            status='pending',
            priority=priority,
        )
        if idempotency_key:
            task_defaults['idempotency_key'] = idempotency_key

        # If idempotency key present and an existing task exists, reuse it
        if idempotency_key:
            existing_idem = AnalysisTask.objects.filter(api_key=request.api_key, idempotency_key=idempotency_key).order_by('-created_at').first()
            if existing_idem:
                queue_position = None
                if existing_idem.status == 'queued':
                    queue_position = existing_idem.queue_position
                    
                return json_success(request, {
                    'task_id': existing_idem.id,
                    'status': existing_idem.status,
                    'queue_position': queue_position,
                    'result_url': request.build_absolute_uri(
                        reverse('task_status_api', args=[existing_idem.id])
                    ),
                    'message': 'Idempotent replay'
                })

        task = AnalysisTask.objects.create(**task_defaults)
        
        print(f"DEBUG: Created new task {task.id} for PURL: {purl}")
        
        # Add task to Celery queue instead of using QueueManager
        try:
            from .tasks import run_dynamic_analysis
            from django.db import transaction
            
            # Mark task as queued and calculate queue position
            with transaction.atomic():
                task.status = 'queued'
                task.queued_at = timezone.now()
                
                # Calculate queue position
                queued_count = AnalysisTask.objects.filter(
                    status='queued'
                ).exclude(id=task.id).count()
                task.queue_position = queued_count + 1
                task.save()
            
            # Queue the task via Celery with priority
            celery_task = run_dynamic_analysis.apply_async(
                args=[task.id],
                priority=task.priority,
                queue='analysis'
            )
            
            logger.info(f"Queued task {task.id} via Celery (Celery ID: {celery_task.id})")
            
            # Return task information
            status_url = request.build_absolute_uri(
                reverse('task_status_api', args=[task.id])
            )

            # Build predicted download URL for the final JSON report
            predicted_download_url = get_predicted_download_url(request, package_name, package_version, ecosystem)

            return json_success(request, {
                'task_id': task.id,
                'status': 'queued',
                'queue_position': task.queue_position,
                'status_url': status_url,
                'result_url': predicted_download_url,
                'message': f'Analysis queued at position {task.queue_position}'
            }, status=202)
            
        except Exception as e:
            import traceback
            logger.error(f"Failed to queue analysis task {task.id}: {e}")
            logger.error(traceback.format_exc())
            
            task.status = 'failed'
            task.error_message = str(e)
            task.completed_at = timezone.now()
            task.save()
            
            return json_error(request, error='Failed to queue analysis', message=str(e), status=500)
    
    except Exception as e:
        # Fallback; decorator also handles this
        return json_error(request, error='Internal server error', message=str(e), status=500)


@csrf_exempt
@api_handler
def task_status_api(request, task_id):
    """
    API endpoint to check analysis task status
    """
    try:
        task = AnalysisTask.objects.get(id=task_id)

        expected_download_url = get_predicted_download_url(request, task.package_name, task.package_version, task.ecosystem)
        response_data = {
            'task_id': task.id,
            'purl': task.purl,
            'status': task.status,
            'created_at': task.created_at.isoformat(),
            'expected_download_url': expected_download_url,
            'package_name': task.package_name,
            'package_version': task.package_version,
            'ecosystem': task.ecosystem,
            'priority': task.priority,
            'queue_position': task.queue_position if task.status == 'queued' else None,
            'queued_at': task.queued_at.isoformat() if task.queued_at else None,
            'timeout_minutes': task.timeout_minutes,
            'container_id': task.container_id,
            'last_heartbeat': task.last_heartbeat.isoformat() if task.last_heartbeat else None
        }
        
        if task.started_at:
            response_data['started_at'] = task.started_at.isoformat()
            
            # Add remaining time for running tasks
            if task.status == 'running':
                remaining_time = task.get_remaining_time_minutes()
                response_data['remaining_time_minutes'] = remaining_time
                response_data['is_timed_out'] = task.is_timed_out()
        
        if task.completed_at:
            response_data['completed_at'] = task.completed_at.isoformat()
        
        if task.error_message:
            response_data['error_message'] = task.error_message
            response_data['error_category'] = task.error_category
            if task.error_details:
                response_data['error_details'] = task.error_details
        
        if task.status == 'completed' and task.report:
            response_data['result_url'] = request.build_absolute_uri(
                reverse('get_report', args=[task.report.id])
            )
            if task.download_url:
                response_data['download_url'] = task.download_url
                # Also provide report metadata if available
                try:
                    import os
                    from django.conf import settings
                    if task.download_url:
                        # Extract filename from download URL
                        filename = os.path.basename(task.download_url)
                        save_dir = getattr(settings, 'MEDIA_ROOT', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'media'))
                        # Try to find the file and get its metadata
                        for root, dirs, files in os.walk(os.path.join(save_dir, 'reports')):
                            if filename in files:
                                file_path = os.path.join(root, filename)
                                response_data['report_metadata'] = {
                                    'filename': filename,
                                    'size_bytes': os.path.getsize(file_path),
                                    'created_at': task.completed_at.isoformat() if task.completed_at else None,
                                    'download_url': task.download_url,
                                    'folder_structure': os.path.relpath(root, save_dir) + '/'
                                }
                                break
                except Exception as e:
                    print(f"Warning: Could not generate report metadata: {e}")
        
        return json_success(request, response_data)
        
    except AnalysisTask.DoesNotExist:
        return json_error(request, error='Task not found', message='Analysis task not found or access denied', status=404)
   


def configure(request):
    return render(request, "package_analysis/configureSubmit.html")

def analyze(request):
    return render(request, "package_analysis/analyzing.html")

def results(request):
    return render(request, "package_analysis/reports.html")




@csrf_exempt
@require_api_key
@api_handler
def list_tasks_api(request):
    """
    Paginated list of analysis tasks for the caller's API key.
    Query params: page (default 1), page_size (default 20, max 100), status
    """
    if request.method != 'GET':
        return json_error(request, error='Method not allowed', message='Only GET requests are supported', status=405)

    try:
        page = int(request.GET.get('page', '1'))
        page_size = min(100, max(1, int(request.GET.get('page_size', '20'))))
    except ValueError:
        return json_error(request, error='Invalid pagination', message='page and page_size must be integers', status=400)

    status_filter = request.GET.get('status')
    qs = AnalysisTask.objects.filter(api_key=request.api_key).order_by('-created_at')
    if status_filter:
        qs = qs.filter(status=status_filter)

    total = qs.count()
    start = (page - 1) * page_size
    end = start + page_size
    items = [
        {
            'task_id': t.id,
            'purl': t.purl,
            'status': t.status,
            'created_at': t.created_at.isoformat(),
            'package_name': t.package_name,
            'package_version': t.package_version,
            'ecosystem': t.ecosystem,
            'priority': t.priority,
            'queue_position': t.queue_position if t.status == 'queued' else None,
            'queued_at': t.queued_at.isoformat() if t.queued_at else None,
            'result_url': (request.build_absolute_uri(reverse('get_report', args=[t.report.id])) if t.report else None),
            'download_url': t.download_url,
            'error_message': t.error_message if t.error_message else None,
            'error_category': t.error_category if t.error_category else None,
        }
        for t in qs[start:end]
    ]

    return json_success(request, {
        'items': items,
        'page': page,
        'page_size': page_size,
        'total': total,
    })


@csrf_exempt
@api_handler
def queue_status_api(request):
    """
    API endpoint to check the current queue status.
    Shows all queued and running tasks across all API keys.
    Now uses direct database queries instead of QueueManager.
    """
    if request.method != 'GET':
        return json_error(request, error='Method not allowed', message='Only GET requests are supported', status=405)
    
    try:
        from django.db import transaction
        
        with transaction.atomic():
            queued_tasks = AnalysisTask.objects.filter(
                status='queued'
            ).order_by('queue_position')
            
            running_tasks = AnalysisTask.objects.filter(
                status='running'
            )
            
            queue_status = {
                'queue_length': queued_tasks.count(),
                'running_tasks': running_tasks.count(),
                'queued_tasks': [
                    {
                        'task_id': task.id,
                        'purl': task.purl,
                        'queue_position': task.queue_position,
                        'priority': task.priority,
                        'queued_at': task.queued_at.isoformat() if task.queued_at else None,
                        'created_at': task.created_at.isoformat()
                    }
                    for task in queued_tasks
                ],
                'running_tasks': [
                    {
                        'task_id': task.id,
                        'purl': task.purl,
                        'started_at': task.started_at.isoformat() if task.started_at else None,
                        'created_at': task.created_at.isoformat()
                    }
                    for task in running_tasks
                ]
            }
        
        return json_success(request, queue_status)
    except Exception as e:
        return json_error(request, error='Failed to get queue status', message=str(e), status=500)


@csrf_exempt
@require_api_key
@api_handler
def task_queue_position_api(request, task_id):
    """
    API endpoint to check the queue position of a specific task.
    Now uses direct database queries instead of QueueManager.
    """
    if request.method != 'GET':
        return json_error(request, error='Method not allowed', message='Only GET requests are supported', status=405)
    
    try:
        task = AnalysisTask.objects.get(id=task_id, api_key=request.api_key)
        
        # Get queue position directly from database
        if task.status == 'queued':
            queue_position = task.queue_position
        elif task.status == 'running':
            queue_position = 0  # Currently running
        else:
            queue_position = None  # Not in queue
        
        return json_success(request, {
            'task_id': task_id,
            'status': task.status,
            'queue_position': queue_position,
            'purl': task.purl,
            'package_name': task.package_name,
            'package_version': task.package_version,
            'ecosystem': task.ecosystem
        })
    except AnalysisTask.DoesNotExist:
        return json_error(request, error='Task not found', message='Analysis task not found or access denied', status=404)
    except Exception as e:
        return json_error(request, error='Failed to get queue position', message=str(e), status=500)


@csrf_exempt
@api_handler
def timeout_status_api(request):
    """
    API endpoint to check timeout status of running tasks.
    Now uses direct database queries instead of QueueManager.
    """
    if request.method != 'GET':
        return json_error(request, error='Method not allowed', message='Only GET requests are supported', status=405)
    
    try:
        from .container_manager import container_manager
        
        running_tasks = AnalysisTask.objects.filter(status='running')
        timeout_info = []
        
        for task in running_tasks:
            remaining_time = task.get_remaining_time_minutes()
            is_timed_out = task.is_timed_out()
            
            timeout_info.append({
                'task_id': task.id,
                'purl': task.purl,
                'started_at': task.started_at.isoformat() if task.started_at else None,
                'timeout_minutes': task.timeout_minutes,
                'remaining_minutes': remaining_time,
                'is_timed_out': is_timed_out,
                'container_id': task.container_id,
                'container_running': container_manager.is_container_running(task.container_id) if task.container_id else False
            })
        
        timeout_status = {
            'running_tasks': len(running_tasks),
            'timed_out_tasks': len([t for t in timeout_info if t['is_timed_out']]),
            'tasks': timeout_info
        }
        
        return json_success(request, timeout_status)
    except Exception as e:
        return json_error(request, error='Failed to get timeout status', message=str(e), status=500)


@csrf_exempt
@api_handler
def check_timeouts_api(request):
    """
    API endpoint to manually trigger timeout check and cleanup.
    Now triggers Celery task instead of QueueManager.
    """
    if request.method != 'POST':
        return json_error(request, error='Method not allowed', message='Only POST requests are supported', status=405)
    
    try:
        # Trigger timeout check via Celery task
        from .tasks import check_timeouts
        result = check_timeouts.delay()
        
        # Get updated status (using same logic as timeout_status_api)
        from .container_manager import container_manager
        
        running_tasks = AnalysisTask.objects.filter(status='running')
        timeout_info = []
        
        for task in running_tasks:
            remaining_time = task.get_remaining_time_minutes()
            is_timed_out = task.is_timed_out()
            
            timeout_info.append({
                'task_id': task.id,
                'purl': task.purl,
                'started_at': task.started_at.isoformat() if task.started_at else None,
                'timeout_minutes': task.timeout_minutes,
                'remaining_minutes': remaining_time,
                'is_timed_out': is_timed_out,
                'container_id': task.container_id,
                'container_running': container_manager.is_container_running(task.container_id) if task.container_id else False
            })
        
        timeout_status = {
            'running_tasks': len(running_tasks),
            'timed_out_tasks': len([t for t in timeout_info if t['is_timed_out']]),
            'tasks': timeout_info
        }
        
        return json_success(request, {
            'message': 'Timeout check queued',
            'celery_task_id': result.id,
            'status': timeout_status
        })
    except Exception as e:
        return json_error(request, error='Failed to check timeouts', message=str(e), status=500)
