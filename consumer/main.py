from tornado.ioloop import IOLoop
from tornado_botocore import Botocore
import botocore
import arrow
import json

from poll import Poll
from update_and_delete import UpdateAndDelete
from delete import Delete

import logging

logging.getLogger(
    'botocore.vendored.requests.packages.urllib3.connectionpool'
).setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


session = botocore.session.get_session()
sqs_receive_message = Botocore(service='sqs',
                               operation='ReceiveMessage',
                               region_name='ap-northeast-1',
                               session=session)
sqs_delete_message_batch = Botocore(service='sqs',
                                    operation='DeleteMessageBatch',
                                    region_name='ap-northeast-1',
                                    session=session)
db_update_item = Botocore(service='dynamodb',
                          operation='UpdateItem',
                          region_name='ap-northeast-1',
                          session=session)
lambda_invoke = Botocore(service='lambda',
                         operation='Invoke',
                         region_name='ap-northeast-1',
                         session=session)


def handler(event, context):
    logger.info("Start!")

    table = event['table']
    queue_url = event['queueUrl']
    message_count = event['messageCount']
    consume(table, queue_url, message_count)

    finish(event)

    deadline = event['deadline']
    check_deadline(deadline)
    return "Lambda job finished successfully."


def consume(table, queue_url, message_count):
    main_loop = IOLoop.instance()

    poll = Poll(main_loop)
    cal = poll.cal
    delete = Delete()
    update_and_delete = UpdateAndDelete(main_loop, delete)

    poll.messages(sqs_receive_message, queue_url, message_count)

    logger.info("Receive API count: {}".format(poll.fetch_count))
    logger.info("Fetched messages: {}".format(poll.message_count))

    update_and_delete.execute(sqs_delete_message_batch, db_update_item,
                              queue_url, table, cal.stats)

    logger.info("Update API count: {}".format(update_and_delete.update_count))
    logger.info("Delete API count: {}".format(delete.delete_count))
    logger.info("Delete Message count: {}".format(delete.message_count))


def finish(event):
    execution_count = event['executionCount']
    next_execution_count = execution_count - 1
    if next_execution_count > 0:
        event['executionCount'] = next_execution_count
        logger.info("Invoke consumer with event: {}".format(event))
        invoke(event)


def invoke(event):
    result = lambda_invoke.call(
        FunctionName=event['consumerArn'],
        InvocationType='Event',
        Payload=json.dumps(event))
    status = result['StatusCode']
    if status > 300:
        logger.warn("Invocation with error response: {}".format(result))
    else:
        logger.info("Invocation succeeded with status code: {}".format(status))


def check_deadline(deadline_str):
    deadline = arrow.get(deadline_str)
    now = arrow.utcnow()
    if now > deadline:
        logger.warn("Failed to finish execution before deadline: {}".format(
            deadline_str))
