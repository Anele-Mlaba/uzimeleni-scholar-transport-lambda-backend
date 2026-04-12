import hashlib
import json
import os
import urllib.parse
import uuid

from common.response import bad_request, created, method_not_allowed, ok

# TODO: Uncomment when DynamoDB is wired up
# import boto3
# from boto3.dynamodb.conditions import Key, Attr
# dynamodb = boto3.resource('dynamodb')
# table = dynamodb.Table(os.environ['TABLE_NAME'])

PAYFAST_SANDBOX_URL = 'https://sandbox.payfast.co.za/eng/process'
PAYFAST_LIVE_URL = 'https://www.payfast.co.za/eng/process'


def lambda_handler(event, context):
    method = event.get('httpMethod', '')
    path = event.get('path', '')

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

    # TODO: Query DynamoDB with optional filters
    # filter_exprs = []
    # if owner_id:
    #     filter_exprs.append(Attr('owner_id').eq(owner_id))
    # if status:
    #     filter_exprs.append(Attr('status').eq(status))
    # combined = filter_exprs[0] if len(filter_exprs) == 1 else filter_exprs[0] & filter_exprs[1]
    # kwargs = {'FilterExpression': combined} if filter_exprs else {}
    # resp = table.query(
    #     IndexName='entity-type-index',
    #     KeyConditionExpression=Key('entity_type').eq('PAYMENT'),
    #     **kwargs,
    # )
    # payments = resp.get('Items', [])
    payments = []
    return ok({'payments': payments, 'count': len(payments)})


def _initiate(event):
    """
    Build a PayFast payment request.

    Expected body:
      {
        "amount":     "150.00",
        "item_name":  "Monthly transport fee",
        "owner_id":   "<uuid>",
        "name_first": "John",        (optional)
        "name_last":  "Doe",         (optional)
        "email":      "j@example.com" (optional)
      }

    Returns the PayFast redirect URL and the pre-signed form data that your
    frontend should POST to PayFast (or redirect to via query string).
    """
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

    # Strip empty values before generating the signature
    payfast_data = {k: v for k, v in payfast_data.items() if v}
    payfast_data['signature'] = _signature(payfast_data, passphrase)

    payfast_url = PAYFAST_SANDBOX_URL if use_sandbox else PAYFAST_LIVE_URL

    # TODO: Persist pending payment in DynamoDB
    # table.put_item(Item={
    #     'pk': f'PAYMENT#{payment_id}',
    #     'sk': 'DETAILS',
    #     'entity_type': 'PAYMENT',
    #     'payment_id': payment_id,
    #     'owner_id': body['owner_id'],
    #     'amount': body['amount'],
    #     'item_name': body['item_name'],
    #     'status': 'pending',
    #     'created_at': _now(),
    # })

    return created({
        'payment_id': payment_id,
        'payfast_url': payfast_url,
        'payfast_data': payfast_data,
    })


def _confirm(event):
    """
    PayFast Instant Transaction Notification (ITN) endpoint.
    PayFast POSTs form-encoded data here after each transaction.
    """
    body_raw = event.get('body', '') or ''

    # PayFast sends application/x-www-form-urlencoded
    try:
        itn = dict(urllib.parse.parse_qsl(body_raw))
    except Exception:
        itn = _parse_body(event)

    payment_id = itn.get('m_payment_id')
    payment_status = itn.get('payment_status', '').lower()

    if not payment_id:
        return bad_request('Missing m_payment_id')

    # TODO: Verify PayFast signature before trusting the notification
    # passphrase = os.environ.get('PAYFAST_PASSPHRASE', '')
    # received_sig = itn.pop('signature', '')
    # if _signature(itn, passphrase) != received_sig:
    #     return bad_request('Invalid signature')

    # TODO: Also validate source IP is a known PayFast server

    # TODO: Update payment status in DynamoDB
    # table.update_item(
    #     Key={'pk': f'PAYMENT#{payment_id}', 'sk': 'DETAILS'},
    #     UpdateExpression='SET #s = :s, updated_at = :ts',
    #     ExpressionAttributeNames={'#s': 'status'},
    #     ExpressionAttributeValues={':s': payment_status, ':ts': _now()},
    # )

    return ok({'message': 'ITN received', 'payment_id': payment_id, 'status': payment_status})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _signature(data: dict, passphrase: str = '') -> str:
    """Generate the MD5 signature PayFast expects."""
    filtered = {k: v for k, v in data.items() if k != 'signature' and str(v) != ''}
    param_string = urllib.parse.urlencode(filtered)
    if passphrase:
        param_string += f'&passphrase={urllib.parse.quote_plus(passphrase)}'
    return hashlib.md5(param_string.encode('utf-8')).hexdigest()


def _parse_body(event):
    try:
        return json.loads(event.get('body') or '{}')
    except (json.JSONDecodeError, TypeError):
        return {}
