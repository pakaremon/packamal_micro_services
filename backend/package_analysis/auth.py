"""
Authentication utilities for API endpoints
"""
from functools import wraps
from django.http import JsonResponse
from django.utils import timezone
from django.core.cache import cache
from .models import APIKey


def require_api_key(view_func):
    """
    Decorator to require valid API key for API endpoints
    """
    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        # Get API key from Authorization header or X-API-Key header
        api_key = None
        
        # Try Authorization header first (Bearer token format)
        auth_header = request.META.get('HTTP_AUTHORIZATION', '')
        if auth_header.startswith('Bearer '):
            api_key = auth_header[7:]
        else:
            # Try X-API-Key header
            api_key = request.META.get('HTTP_X_API_KEY')
        
        if not api_key:
            return JsonResponse({
                'error': 'API key required',
                'message': 'Please provide API key in Authorization header (Bearer <key>) or X-API-Key header'
            }, status=401)
        
        # Validate API key
        try:
            api_key_obj = APIKey.objects.get(key=api_key, is_active=True)
        except APIKey.DoesNotExist:
            return JsonResponse({
                'error': 'Invalid API key',
                'message': 'The provided API key is invalid or inactive'
            }, status=401)
        
        # Check rate limiting
        rate_limit_exceeded = check_rate_limit(api_key_obj)
        if rate_limit_exceeded:
            return JsonResponse({
                'error': 'Rate limit exceeded',
                'message': f'Maximum {api_key_obj.rate_limit_per_hour} requests per hour exceeded'
            }, status=429)
        
        # Update last used timestamp
        api_key_obj.last_used = timezone.now()
        api_key_obj.save(update_fields=['last_used'])
        
        # Add API key object to request for use in view
        request.api_key = api_key_obj
        
        return view_func(request, *args, **kwargs)
    
    return wrapper


def check_rate_limit(api_key_obj: APIKey) -> bool:
    """
    Check if API key has exceeded rate limit
    
    Returns:
        True if rate limit exceeded, False otherwise
    """
    cache_key = f"api_rate_limit_{api_key_obj.key}"
    
    # Get current request count from cache
    current_count = cache.get(cache_key, 0)
    
    # Check if limit exceeded
    if current_count >= api_key_obj.rate_limit_per_hour:
        return True
    
    # Increment counter and set expiry to 1 hour
    cache.set(cache_key, current_count + 1, 3600)
    
    return False


def get_api_key_from_request(request):
    """
    Extract API key from request headers
    
    Returns:
        APIKey object if valid, None otherwise
    """
    api_key = None
    
    # Try Authorization header first (Bearer token format)
    auth_header = request.META.get('HTTP_AUTHORIZATION', '')
    if auth_header.startswith('Bearer '):
        api_key = auth_header[7:]
    else:
        # Try X-API-Key header
        api_key = request.META.get('HTTP_X_API_KEY')
    
    if not api_key:
        return None
    
    try:
        return APIKey.objects.get(key=api_key, is_active=True)
    except APIKey.DoesNotExist:
        return None

