import json

import config


def new_control_event(queue_url, deadline, execution_count):
    return json.dumps({
        'consumerArn': config.consumer_arn,
        'executionCount': execution_count,
        'deadline': deadline,
        'messageCount': config.message_batch_count,
        'queueUrl': queue_url,
        'table': config.output_table,
    })
