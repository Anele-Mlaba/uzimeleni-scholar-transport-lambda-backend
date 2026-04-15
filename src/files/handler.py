import json
import os

import boto3
from botocore.exceptions import ClientError

from common.response import bad_request, method_not_allowed, ok, server_error

s3 = boto3.client('s3')
BUCKET = os.environ['S3_BUCKET_NAME']
EXPIRY = 3600  # presigned URL lifetime in seconds (1 hour)


def lambda_handler(event, context):
    method = event.get('httpMethod', '')

    if method == 'GET':
        return _get_download_url(event)
    if method == 'POST':
        return _get_upload_url(event)
    if method == 'DELETE':
        return _get_delete_url(event)

    return method_not_allowed()


# ── Handlers ──────────────────────────────────────────────────────────────────

def _get_download_url(event):
    """Return a presigned GET URL the frontend can use to download a file."""
    params = event.get('queryStringParameters') or {}
    folder = params.get('folder', '').strip()
    file = params.get('file', '').strip()

    print("----------------------")
    print(folder)
    print(file)
    err = _require_path(folder, file)
    if err:
        return bad_request(err)

    key = f'{folder}/{file}'

    try:
        url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': BUCKET, 'Key': key},
            ExpiresIn=EXPIRY,
        )
    except ClientError as e:
        return server_error(e.response['Error']['Message'])

    return ok({'url': url, 'key': key, 'expires_in': EXPIRY, 'action': 'download'})


def _get_upload_url(event):
    """Return a presigned POST URL + fields the frontend can use to upload a file directly to S3."""
    body = _parse_body(event)
    folder = (body.get('folder') or '').strip()
    file = (body.get('file') or '').strip()
    content_type = (body.get('content_type') or 'application/octet-stream').strip()

    err = _require_path(folder, file)
    if err:
        return bad_request(err)

    key = f'{folder}/{file}'

    try:
        result = s3.generate_presigned_post(
            Bucket=BUCKET,
            Key=key,
            Fields={'Content-Type': content_type},
            Conditions=[{'Content-Type': content_type}],
            ExpiresIn=EXPIRY,
        )
    except ClientError as e:
        return server_error(e.response['Error']['Message'])

    return ok({
        'url': result['url'],
        'fields': result['fields'],
        'key': key,
        'expires_in': EXPIRY,
        'action': 'upload',
    })


def _get_delete_url(event):
    """Return a presigned DELETE URL the frontend can use to remove a file from S3."""
    # Accept folder/file from query params or JSON body (DELETE + body is valid HTTP)
    params = event.get('queryStringParameters') or {}
    folder = params.get('folder', '').strip()
    file = params.get('file', '').strip()

    if not folder or not file:
        body = _parse_body(event)
        folder = (body.get('folder') or '').strip()
        file = (body.get('file') or '').strip()

    err = _require_path(folder, file)
    if err:
        return bad_request(err)

    key = f'{folder}/{file}'

    try:
        url = s3.generate_presigned_url(
            'delete_object',
            Params={'Bucket': BUCKET, 'Key': key},
            ExpiresIn=EXPIRY,
        )
    except ClientError as e:
        return server_error(e.response['Error']['Message'])

    return ok({'url': url, 'key': key, 'expires_in': EXPIRY, 'action': 'delete'})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _require_path(folder, file):
    """Validate folder and file values; return an error string or None."""
    if not folder or not file:
        return 'folder and file are required'
    if '..' in folder or '..' in file:
        return 'folder and file must not contain ..'
    if folder.startswith('/') or file.startswith('/'):
        return 'folder and file must not start with /'
    return None


def _parse_body(event):
    try:
        return json.loads(event.get('body') or '{}')
    except (json.JSONDecodeError, TypeError):
        return {}
