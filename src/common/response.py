import json

_CORS_HEADERS = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
    'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
}


def ok(body):
    return _build(200, body)


def created(body):
    return _build(201, body)


def bad_request(message):
    return _build(400, {'error': message})


def unauthorized(message='Unauthorized'):
    return _build(401, {'error': message})


def not_found(message='Resource not found'):
    return _build(404, {'error': message})


def method_not_allowed():
    return _build(405, {'error': 'Method not allowed'})


def server_error(message='Internal server error'):
    return _build(500, {'error': message})


def _build(status_code, body):
    return {
        'statusCode': status_code,
        'headers': _CORS_HEADERS,
        'body': json.dumps(body),
    }
