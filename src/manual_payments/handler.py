import json
import logging
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

VALID_REASONS = {'late', 'absent', 'custom'}
LATE_DEFAULT_AMOUNT = 50
ABSENT_DEFAULT_AMOUNT = 200


def lambda_handler(event, context):
    method = event.get('httpMethod', '')
    path = event.get('path', '')
    path_params = event.get('pathParameters') or {}
    payment_id = path_params.get('id')
    logger.info('Request: %s %s', method, path)

    if payment_id:
        if path.endswith('/pay'):
            if method == 'PUT':
                return _mark_paid(payment_id, event)
        else:
            if method == 'GET':
                return _get(payment_id)
            if method == 'DELETE':
                return _cancel(payment_id)
    else:
        if method == 'GET':
            return _list(event)
        if method == 'POST':
            if path.endswith('/bulk'):
                return _bulk_create(event)
            return _create(event)

    return method_not_allowed()


# ── Handlers ──────────────────────────────────────────────────────────────────

def _list(event):
    query_params = event.get('queryStringParameters') or {}
    id_number = query_params.get('id_number')
    status = query_params.get('status')
    meeting_id = query_params.get('meeting_id')
    reason = query_params.get('reason')

    filter_expr = Attr('entity_type').eq('MANUAL_PAYMENT')
    if id_number:
        filter_expr = filter_expr & Attr('id_number').eq(id_number)
    if status:
        filter_expr = filter_expr & Attr('status').eq(status)
    if meeting_id:
        filter_expr = filter_expr & Attr('meeting_id').eq(meeting_id)
    if reason:
        filter_expr = filter_expr & Attr('reason').eq(reason)

    try:
        resp = table.scan(FilterExpression=filter_expr)
    except ClientError as e:
        logger.error('DynamoDB scan failed: %s', e.response['Error'])
        return server_error()

    payments = resp.get('Items', [])
    return ok({'payments': payments, 'count': len(payments)})


def _get(payment_id):
    try:
        resp = table.get_item(Key={'PK': f'MANUAL_PAYMENT#{payment_id}', 'SK': 'DETAILS'})
    except ClientError as e:
        logger.error('DynamoDB get_item failed: %s', e.response['Error'])
        return server_error()

    payment = resp.get('Item')
    if not payment:
        return not_found(f'Payment {payment_id} not found')

    return ok({'payment': payment})


def _create(event):
    body = _parse_body(event)

    id_number = body.get('id_number')
    reason = body.get('reason')

    if not id_number:
        return bad_request('id_number is required')
    if not reason or reason not in VALID_REASONS:
        return bad_request(f'reason must be one of: {", ".join(VALID_REASONS)}')

    # Apply default amounts for late/absent if not explicitly provided
    if reason == 'late':
        amount = body.get('amount', LATE_DEFAULT_AMOUNT)
        description = body.get('description', 'Late arrival fee')
    elif reason == 'absent':
        amount = body.get('amount', ABSENT_DEFAULT_AMOUNT)
        description = body.get('description', 'Absence fee')
    else:
        amount = body.get('amount')
        description = body.get('description', '')
        if not amount:
            return bad_request('amount is required for custom payments')

    payment_id = str(uuid.uuid4())
    logger.info('Creating manual payment: payment_id=%s id_number=%s reason=%s amount=%s',
                payment_id, id_number, reason, amount)

    item = {
        'PK': f'MANUAL_PAYMENT#{payment_id}',
        'SK': 'DETAILS',
        'entity_type': 'MANUAL_PAYMENT',
        'payment_id': payment_id,
        'id_number': id_number,
        'amount': str(amount),
        'reason': reason,
        'description': description,
        'status': 'outstanding',
        'created_at': _now(),
    }

    if body.get('meeting_id'):
        item['meeting_id'] = body['meeting_id']
    if body.get('notes'):
        item['notes'] = body['notes']

    try:
        table.put_item(Item=item)
    except ClientError as e:
        logger.error('DynamoDB put_item failed: %s', e.response['Error'])
        return server_error()

    logger.info('Manual payment created: payment_id=%s', payment_id)
    return created({'message': 'Payment created', 'payment_id': payment_id})


def _mark_paid(payment_id, event):
    body = _parse_body(event)

    logger.info('Marking payment as paid: payment_id=%s', payment_id)

    update_expr = 'SET #s = :s, paid_at = :ts'
    expr_names = {'#s': 'status'}
    expr_values = {':s': 'paid', ':ts': _now()}

    if body.get('notes'):
        update_expr += ', notes = :n'
        expr_values[':n'] = body['notes']

    try:
        table.update_item(
            Key={'PK': f'MANUAL_PAYMENT#{payment_id}', 'SK': 'DETAILS'},
            UpdateExpression=update_expr,
            ConditionExpression=Attr('status').eq('outstanding'),
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_values,
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return bad_request('Payment is not in outstanding status')
        logger.error('DynamoDB update_item failed: %s', e.response['Error'])
        return server_error()

    logger.info('Payment marked as paid: payment_id=%s', payment_id)
    return ok({'message': 'Payment marked as paid', 'payment_id': payment_id})


def _cancel(payment_id):
    logger.info('Cancelling payment: payment_id=%s', payment_id)

    try:
        table.update_item(
            Key={'PK': f'MANUAL_PAYMENT#{payment_id}', 'SK': 'DETAILS'},
            UpdateExpression='SET #s = :s, updated_at = :ts',
            ConditionExpression=Attr('payment_id').exists() & Attr('status').eq('outstanding'),
            ExpressionAttributeNames={'#s': 'status'},
            ExpressionAttributeValues={':s': 'cancelled', ':ts': _now()},
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return bad_request('Payment not found or already settled')
        logger.error('DynamoDB update_item failed: %s', e.response['Error'])
        return server_error()

    logger.info('Payment cancelled: payment_id=%s', payment_id)
    return ok({'message': 'Payment cancelled', 'payment_id': payment_id})


def _bulk_create(event):
    body = _parse_body(event)

    reason = body.get('reason')
    if not reason or reason not in VALID_REASONS:
        return bad_request(f'reason must be one of: {", ".join(VALID_REASONS)}')

    if reason == 'late':
        amount = body.get('amount', LATE_DEFAULT_AMOUNT)
        description = body.get('description', 'Late arrival fee')
    elif reason == 'absent':
        amount = body.get('amount', ABSENT_DEFAULT_AMOUNT)
        description = body.get('description', 'Absence fee')
    else:
        amount = body.get('amount')
        description = body.get('description', '')
        if not amount:
            return bad_request('amount is required for custom payments')

    try:
        resp = table.scan(FilterExpression=Attr('entity_type').eq('OWNER'))
    except ClientError as e:
        logger.error('DynamoDB scan failed: %s', e.response['Error'])
        return server_error()

    owners = resp.get('Items', [])
    if not owners:
        return bad_request('No owners found in the organisation')

    now = _now()
    meeting_id = body.get('meeting_id')
    notes = body.get('notes')

    created_ids = []
    try:
        with table.batch_writer() as batch:
            for owner in owners:
                payment_id = str(uuid.uuid4())
                item = {
                    'PK': f'MANUAL_PAYMENT#{payment_id}',
                    'SK': 'DETAILS',
                    'entity_type': 'MANUAL_PAYMENT',
                    'payment_id': payment_id,
                    'id_number': owner['id_number'],
                    'amount': str(amount),
                    'reason': reason,
                    'description': description,
                    'status': 'outstanding',
                    'created_at': now,
                }
                if meeting_id:
                    item['meeting_id'] = meeting_id
                if notes:
                    item['notes'] = notes
                batch.put_item(Item=item)
                created_ids.append(payment_id)
    except ClientError as e:
        logger.error('DynamoDB batch_write failed: %s', e.response['Error'])
        return server_error()

    logger.info('Bulk payments created: count=%d reason=%s', len(created_ids), reason)
    return created({
        'message': f'{len(created_ids)} payments created',
        'count': len(created_ids),
        'payment_ids': created_ids,
    })


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_body(event):
    try:
        return json.loads(event.get('body') or '{}')
    except (json.JSONDecodeError, TypeError):
        logger.warning('Failed to parse request body')
        return {}


def _now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
