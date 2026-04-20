import json
import logging
import os
import uuid
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from common.response import bad_request, created, method_not_allowed, not_found, ok, server_error

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('zimeleni-transport')


def lambda_handler(event, context):
    method = event.get('httpMethod', '')
    path_params = event.get('pathParameters') or {}
    owner_id = path_params.get('id')
    logger.info('Request: %s /owners%s', method, f'/{owner_id}' if owner_id else '')

    if owner_id:
        if method == 'GET':
            return _get(owner_id)
        if method == 'PUT':
            return _update(owner_id, event)
        if method == 'DELETE':
            return _delete(owner_id)
    else:
        if method == 'GET':
            return _list()
        if method == 'POST':
            return _create(event)

    return method_not_allowed()


# ── Handlers ──────────────────────────────────────────────────────────────────

def _list():
    try:
        resp = table.scan(
            FilterExpression=Attr('entity_type').eq('OWNER'),
        )
    except ClientError as e:
        logger.error('DynamoDB scan failed: %s', e.response['Error'])
        return server_error()

    owners = resp.get('Items', [])
    return ok({'owners': owners, 'count': len(owners)})


def _get(owner_id):
    try:
        resp = table.get_item(Key={'PK': f'OWNER#{owner_id}', 'SK': 'PROFILE'})
    except ClientError as e:
        logger.error('DynamoDB get_item failed: %s', e.response['Error'])
        return server_error()

    owner = resp.get('Item')
    if not owner:
        logger.warning('Owner not found: owner_id=%s', owner_id)
        return not_found(f'Owner {owner_id} not found')

    return ok({'owner': owner})


def _create(event):
    body = _parse_body(event)

    for field in ('name', 'id_number', 'phone', 'email'):
        if not body.get(field):
            return bad_request(f'{field} is required')

    owner_id = str(uuid.uuid4())
    logger.info('Creating owner: owner_id=%s', owner_id)

    try:
        table.put_item(Item={
            'PK': f'OWNER#{owner_id}',
            'SK': 'PROFILE',
            'entity_type': 'OWNER',
            'owner_id': owner_id,
            'name': body['name'],
            'id_number': body['id_number'],
            'phone': body['phone'],
            'email': body['email'],
            'address': body.get('address', ''),
            'created_at': _now(),
        })
    except ClientError as e:
        logger.error('DynamoDB put_item failed: %s', e.response['Error'])
        return server_error()

    logger.info('Owner created: owner_id=%s', owner_id)
    return created({'message': 'Owner created', 'owner_id': owner_id})


def _update(owner_id, event):
    body = _parse_body(event)
    if not body:
        return bad_request('Request body is required')

    logger.info('Updating owner: owner_id=%s', owner_id)

    try:
        table.update_item(
            Key={'PK': f'OWNER#{owner_id}', 'SK': 'PROFILE'},
            UpdateExpression='SET #n = :name, phone = :phone, email = :email, updated_at = :ts',
            ExpressionAttributeNames={'#n': 'name'},
            ExpressionAttributeValues={
                ':name': body.get('name'),
                ':phone': body.get('phone'),
                ':email': body.get('email'),
                ':ts': _now(),
            },
        )
    except ClientError as e:
        logger.error('DynamoDB update_item failed: %s', e.response['Error'])
        return server_error()

    logger.info('Owner updated: owner_id=%s', owner_id)
    return ok({'message': 'Owner updated', 'owner_id': owner_id})


def _delete(owner_id):
    logger.info('Deleting owner: owner_id=%s', owner_id)

    try:
        table.delete_item(Key={'PK': f'OWNER#{owner_id}', 'SK': 'PROFILE'})
    except ClientError as e:
        logger.error('DynamoDB delete_item failed: %s', e.response['Error'])
        return server_error()

    logger.info('Owner deleted: owner_id=%s', owner_id)
    return ok({'message': 'Owner deleted', 'owner_id': owner_id})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_body(event):
    try:
        return json.loads(event.get('body') or '{}')
    except (json.JSONDecodeError, TypeError):
        logger.warning('Failed to parse request body')
        return {}


def _now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
