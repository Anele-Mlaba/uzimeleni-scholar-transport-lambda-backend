import hashlib
import json
import uuid

from common.response import bad_request, created, method_not_allowed, ok, unauthorized

# TODO: Uncomment when DynamoDB is wired up
# import os
# import boto3
# from boto3.dynamodb.conditions import Key
# dynamodb = boto3.resource('dynamodb')
# table = dynamodb.Table(os.environ['TABLE_NAME'])


def lambda_handler(event, context):
    path = event.get('path', '')
    method = event.get('httpMethod', '')

    if path.endswith('/login') and method == 'POST':
        return _login(event)
    if path.endswith('/register') and method == 'POST':
        return _register(event)
    if path.endswith('/logout') and method == 'POST':
        return _logout(event)

    return method_not_allowed()


# ── Handlers ──────────────────────────────────────────────────────────────────

def _login(event):
    body = _parse_body(event)
    email = body.get('email')
    password = body.get('password')

    if not email or not password:
        return bad_request('email and password are required')

    # TODO: Fetch user from DynamoDB and verify password hash
    # resp = table.get_item(Key={'pk': f'USER#{email}', 'sk': 'PROFILE'})
    # user = resp.get('Item')
    # if not user or user.get('password_hash') != _hash(password):
    #     return unauthorized('Invalid credentials')

    # TODO: Replace placeholder token with a signed JWT
    token = f'placeholder_token_{uuid.uuid4().hex}'

    return ok({'message': 'Login successful', 'token': token})


def _register(event):
    body = _parse_body(event)

    for field in ('email', 'password', 'name'):
        if not body.get(field):
            return bad_request(f'{field} is required')

    user_id = str(uuid.uuid4())

    # TODO: Store user in DynamoDB (never store plaintext passwords)
    # table.put_item(Item={
    #     'pk': f'USER#{body["email"]}',
    #     'sk': 'PROFILE',
    #     'user_id': user_id,
    #     'name': body['name'],
    #     'email': body['email'],
    #     'password_hash': _hash(body['password']),
    #     'role': body.get('role', 'user'),
    #     'created_at': _now(),
    # })

    return created({'message': 'User registered successfully', 'user_id': user_id})


def _logout(event):
    # TODO: Invalidate the token (blacklist in DynamoDB or use short-lived JWTs)
    return ok({'message': 'Logged out successfully'})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_body(event):
    try:
        return json.loads(event.get('body') or '{}')
    except (json.JSONDecodeError, TypeError):
        return {}


def _hash(value):
    return hashlib.sha256(value.encode('utf-8')).hexdigest()
