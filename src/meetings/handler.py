import json
import uuid

from common.response import bad_request, created, method_not_allowed, not_found, ok

# TODO: Uncomment when DynamoDB is wired up
# import os
# import boto3
# from boto3.dynamodb.conditions import Key
# dynamodb = boto3.resource('dynamodb')
# table = dynamodb.Table(os.environ['TABLE_NAME'])


def lambda_handler(event, context):
    method = event.get('httpMethod', '')
    path = event.get('path', '')
    path_params = event.get('pathParameters') or {}
    meeting_id = path_params.get('id')

    if meeting_id and path.endswith('/attendance'):
        # /meetings/{id}/attendance
        if method == 'POST':
            return _record_attendance(meeting_id, event)
    elif meeting_id:
        # /meetings/{id}
        if method == 'PUT':
            return _update(meeting_id, event)
    else:
        # /meetings
        if method == 'GET':
            return _list()
        if method == 'POST':
            return _create(event)

    return method_not_allowed()


# ── Handlers ──────────────────────────────────────────────────────────────────

def _list():
    # TODO: Query DynamoDB
    # resp = table.query(
    #     IndexName='entity-type-index',
    #     KeyConditionExpression=Key('entity_type').eq('MEETING'),
    #     ScanIndexForward=False,  # newest first
    # )
    # meetings = resp.get('Items', [])
    meetings = []
    return ok({'meetings': meetings, 'count': len(meetings)})


def _create(event):
    body = _parse_body(event)

    for field in ('title', 'date', 'location'):
        if not body.get(field):
            return bad_request(f'{field} is required')

    meeting_id = str(uuid.uuid4())

    # TODO: Store in DynamoDB
    # table.put_item(Item={
    #     'pk': f'MEETING#{meeting_id}',
    #     'sk': 'DETAILS',
    #     'entity_type': 'MEETING',
    #     'meeting_id': meeting_id,
    #     'title': body['title'],
    #     'date': body['date'],
    #     'location': body['location'],
    #     'agenda': body.get('agenda', ''),
    #     'created_at': _now(),
    # })

    return created({'message': 'Meeting created', 'meeting_id': meeting_id})


def _update(meeting_id, event):
    body = _parse_body(event)
    if not body:
        return bad_request('Request body is required')

    # TODO: Update in DynamoDB
    # table.update_item(
    #     Key={'pk': f'MEETING#{meeting_id}', 'sk': 'DETAILS'},
    #     UpdateExpression='SET title = :t, #d = :d, location = :l, updated_at = :ts',
    #     ExpressionAttributeNames={'#d': 'date'},
    #     ExpressionAttributeValues={
    #         ':t': body.get('title'),
    #         ':d': body.get('date'),
    #         ':l': body.get('location'),
    #         ':ts': _now(),
    #     },
    # )

    return ok({'message': 'Meeting updated', 'meeting_id': meeting_id})


def _record_attendance(meeting_id, event):
    body = _parse_body(event)
    attendee_ids = body.get('attendee_ids', [])

    if not attendee_ids or not isinstance(attendee_ids, list):
        return bad_request('attendee_ids must be a non-empty list')

    # TODO: Write one attendance record per attendee in DynamoDB
    # with table.batch_writer() as batch:
    #     for attendee_id in attendee_ids:
    #         batch.put_item(Item={
    #             'pk': f'MEETING#{meeting_id}',
    #             'sk': f'ATTENDANCE#{attendee_id}',
    #             'entity_type': 'ATTENDANCE',
    #             'meeting_id': meeting_id,
    #             'attendee_id': attendee_id,
    #             'recorded_at': _now(),
    #         })

    return created({
        'message': 'Attendance recorded',
        'meeting_id': meeting_id,
        'attendee_count': len(attendee_ids),
    })


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_body(event):
    try:
        return json.loads(event.get('body') or '{}')
    except (json.JSONDecodeError, TypeError):
        return {}
