import uuid
from typing import Any, Dict, Optional
from django.http import JsonResponse


def _with_request_id(payload: Dict[str, Any], request) -> Dict[str, Any]:
    request_id = request.META.get('HTTP_X_REQUEST_ID') or str(uuid.uuid4())
    payload.setdefault('request_id', request_id)
    return payload


def json_success(request, data: Dict[str, Any], status: int = 200, headers: Optional[Dict[str, str]] = None) -> JsonResponse:
    body = {
        'success': True,
        'data': data,
    }
    body = _with_request_id(body, request)
    response = JsonResponse(body, status=status)
    if headers:
        for key, value in headers.items():
            response[key] = value
    return response


def json_error(request, *, error: str, message: str, status: int, code: Optional[str] = None, fields: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> JsonResponse:
    body: Dict[str, Any] = {
        'success': False,
        'error': error,
        'message': message,
    }
    if code:
        body['code'] = code
    if fields:
        body['fields'] = fields
    body = _with_request_id(body, request)
    response = JsonResponse(body, status=status)
    if headers:
        for key, value in headers.items():
            response[key] = value
    return response


def api_handler(view_func):
    """Decorator to standardize API error handling."""
    from functools import wraps
    import json

    @wraps(view_func)
    def _wrapped(request, *args, **kwargs):
        try:
            return view_func(request, *args, **kwargs)
        except json.JSONDecodeError:
            return json_error(request, error='Invalid JSON', message='Request body must be valid JSON', status=400)
        except Exception as exc:  # noqa: BLE001 broad except at boundary
            # Do not leak internal errors; return minimal info
            return json_error(request, error='Internal server error', message=str(exc), status=500)

    return _wrapped


