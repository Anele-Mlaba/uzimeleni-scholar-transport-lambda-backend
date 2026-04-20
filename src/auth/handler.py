import hashlib
import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

import boto3
import jwt
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from common.response import bad_request, conflict, created, method_not_allowed, ok, server_error, unauthorized

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('zimeleni-transport')


def lambda_handler(event, context):
    path = event.get('path', '')
    method = event.get('httpMethod', '')
    logger.info('Request: %s %s', method, path)

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
    id_number = body.get('id_number')
    password = body.get('password')

    if not id_number or not password:
        return bad_request('id_number and password are required')

    try:
        resp = table.get_item(Key={'PK': f'USER#{id_number}', 'SK': 'PROFILE'})
    except ClientError as e:
        logger.error('DynamoDB get_item failed: %s', e.response['Error'])
        return server_error()

    user = resp.get('Item')
    if not user or user.get('password_hash') != _hash(password):
        logger.warning('Failed login attempt for id_number=%s', id_number)
        return unauthorized('Invalid credentials')

    now = datetime.now(timezone.utc)
    payload = {
        'sub': id_number,
        'name': user.get('name'),
        'role': user.get('role', 'user'),
        'iat': now,
        'exp': now + timedelta(hours=24),
    }
    token = jwt.encode(payload, os.environ['JWT_SECRET'], algorithm='HS256')
    logger.info('Login successful for id_number=%s', id_number)
    return ok({'message': 'Login successful', 'token': token})


def _register(event):
    body = _parse_body(event)

    for field in ('id_number', 'name', 'password'):
        if not body.get(field):
            return bad_request(f'{field} is required')

    id_number = body['id_number']
    logger.info('Register attempt for id_number=%s', id_number)

    try:
        resp = table.scan(
            FilterExpression=Attr('id_number').eq(id_number) & Attr('entity_type').is_in(['OWNER']),
        )
    except ClientError as e:
        logger.error('DynamoDB scan failed: %s', e.response['Error'])
        return server_error()

    if not resp.get('Items'):
        logger.warning('Register failed: id_number=%s not found in system', id_number)
        return bad_request('ID number is not registered in the system')

    try:
        existing = table.get_item(Key={'PK': f'USER#{id_number}', 'SK': 'PROFILE'})
    except ClientError as e:
        logger.error('DynamoDB get_item failed: %s', e.response['Error'])
        return server_error()

    if existing.get('Item'):
        logger.warning('Register failed: account already exists for id_number=%s', id_number)
        return conflict('An account already exists for this ID number')

    user_id = str(uuid.uuid4())
    try:
        table.put_item(Item={
            'PK': f'USER#{id_number}',
            'SK': 'PROFILE',
            'entity_type': 'USER',
            'user_id': user_id,
            'id_number': id_number,
            'name': body['name'],
            'password_hash': _hash(body['password']),
            'role': 'owner',
            'created_at': _now(),
        })
    except ClientError as e:
        logger.error('DynamoDB put_item failed: %s', e.response['Error'])
        return server_error()

    logger.info('User registered: user_id=%s id_number=%s', user_id, id_number)
    return created({'message': 'User registered successfully', 'user_id': user_id})


def _logout(event):
    # TODO: Invalidate the token (blacklist in DynamoDB or use short-lived JWTs)
    return ok({'message': 'Logged out successfully'})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_body(event):
    try:
        return json.loads(event.get('body') or '{}')
    except (json.JSONDecodeError, TypeError):
        logger.warning('Failed to parse request body')
        return {}


def _hash(value):
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def _now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
