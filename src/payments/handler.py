import hashlib
import json
import logging
import os
import urllib.parse
import uuid
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from common.response import bad_request, created, method_not_allowed, ok, server_error

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('zimeleni-transport')

PAYFAST_SANDBOX_URL = 'https://sandbox.payfast.co.za/eng/process'
PAYFAST_LIVE_URL = 'https://www.payfast.co.za/eng/process'


def lambda_handler(event, context):
    method = event.get('httpMethod', '')
    path = event.get('path', '')
    logger.info('Request: %s %s', method, path)

    if method == 'GET' and path.endswith('/payments'):
        return _list(event)
    if method == 'POST' and path.endswith('/initiate'):
        return _initiate(event)
    if method == 'POST' and path.endswith('/confirm'):
        return _confirm(event)

    return method_not_allowed()


# ── Handlers ──────────────────────────────────────────────────────────────────

def _list(event):
    query_params = event.get('queryStringParameters') or {}
    owner_id = query_params.get('owner_id')
    status = query_params.get('status')

    filter_expr = Attr('entity_type').eq('PAYMENT')
    if owner_id:
        filter_expr = filter_expr & Attr('owner_id').eq(owner_id)
    if status:
        filter_expr = filter_expr & Attr('status').eq(status)

    try:
        resp = table.scan(FilterExpression=filter_expr)
    except ClientError as e:
        logger.error('DynamoDB scan failed: %s', e.response['Error'])
        return server_error()

    payments = resp.get('Items', [])
    return ok({'payments': payments, 'count': len(payments)})


def _initiate(event):
    body = _parse_body(event)

    for field in ('amount', 'item_name', 'owner_id'):
        if not body.get(field):
            return bad_request(f'{field} is required')

    merchant_id = os.environ.get('PAYFAST_MERCHANT_ID', '')
    merchant_key = os.environ.get('PAYFAST_MERCHANT_KEY', '')
    passphrase = os.environ.get('PAYFAST_PASSPHRASE', '')
    api_base = os.environ.get('API_BASE_URL', '')
    use_sandbox = os.environ.get('PAYFAST_SANDBOX', 'true').lower() == 'true'

    payment_id = str(uuid.uuid4())
    logger.info('Initiating payment: payment_id=%s owner_id=%s', payment_id, body['owner_id'])

    payfast_data = {
        'merchant_id': merchant_id,
        'merchant_key': merchant_key,
        'return_url': f'{api_base}/payments/success',
        'cancel_url': f'{api_base}/payments/cancel',
        'notify_url': f'{api_base}/payments/confirm',
        'name_first': body.get('name_first', ''),
        'name_last': body.get('name_last', ''),
        'email_address': body.get('email', ''),
        'm_payment_id': payment_id,
        'amount': f"{float(body['amount']):.2f}",
        'item_name': body['item_name'],
    }

    payfast_data = {k: v for k, v in payfast_data.items() if v}
    payfast_data['signature'] = _signature(payfast_data, passphrase)

    payfast_url = PAYFAST_SANDBOX_URL if use_sandbox else PAYFAST_LIVE_URL

    try:
        table.put_item(Item={
            'PK': f'PAYMENT#{payment_id}',
            'SK': 'DETAILS',
            'entity_type': 'PAYMENT',
            'payment_id': payment_id,
            'owner_id': body['owner_id'],
            'amount': body['amount'],
            'item_name': body['item_name'],
            'status': 'pending',
            'created_at': _now(),
        })
    except ClientError as e:
        logger.error('DynamoDB put_item failed: %s', e.response['Error'])
        return server_error()

    logger.info('Payment initiated: payment_id=%s', payment_id)
    return created({
        'payment_id': payment_id,
        'payfast_url': payfast_url,
        'payfast_data': payfast_data,
    })


def _confirm(event):
    body_raw = event.get('body', '') or ''

    try:
        itn = dict(urllib.parse.parse_qsl(body_raw))
    except Exception:
        itn = _parse_body(event)

    payment_id = itn.get('m_payment_id')
    payment_status = itn.get('payment_status', '').lower()

    if not payment_id:
        return bad_request('Missing m_payment_id')

    logger.info('ITN received: payment_id=%s status=%s', payment_id, payment_status)

    try:
        table.update_item(
            Key={'PK': f'PAYMENT#{payment_id}', 'SK': 'DETAILS'},
            UpdateExpression='SET #s = :s, updated_at = :ts',
            ExpressionAttributeNames={'#s': 'status'},
            ExpressionAttributeValues={':s': payment_status, ':ts': _now()},
        )
    except ClientError as e:
        logger.error('DynamoDB update_item failed: %s', e.response['Error'])
        return server_error()

    return ok({'message': 'ITN received', 'payment_id': payment_id, 'status': payment_status})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _signature(data: dict, passphrase: str = '') -> str:
    filtered = {k: v for k, v in data.items() if k != 'signature' and str(v) != ''}
    param_string = urllib.parse.urlencode(filtered)
    if passphrase:
        param_string += f'&passphrase={urllib.parse.quote_plus(passphrase)}'
    return hashlib.md5(param_string.encode('utf-8')).hexdigest()


def _parse_body(event):
    try:
        return json.loads(event.get('body') or '{}')
    except (json.JSONDecodeError, TypeError):
        logger.warning('Failed to parse request body')
        return {}


def _now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
