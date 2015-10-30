from tornado.ioloop import IOLoop
from tornado_botocore import Botocore
from tornado import gen
import botocore
import arrow
import uuid

import logging

import config
import event

logging.getLogger(
    'botocore.vendored.requests.packages.urllib3.connectionpool'
).setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


session = botocore.session.get_session()
sqs_number_of_visible_message = Botocore(service='sqs',
                                         operation='GetQueueAttributes',
                                         region_name='ap-northeast-1',
                                         session=session)
lambda_invoke = Botocore(service='lambda',
                         operation='Invoke',
                         region_name='ap-northeast-1',
                         session=session)


def handler(event, contest):
    logger.info("Start!")

    time = arrow.get(event['time'])
    deadline = (time + config.job_interval).format('YYYY-MM-DDTHH:mm:ss') + 'Z'

    loop = IOLoop.instance()

    scheduler = Scheduler(config.queue_urls, deadline)

    loop.run_sync(scheduler.schedule_queue_consumers)

    return "Lambda job finished successfully."


class Scheduler(object):

    def __init__(self, queue_urls, deadline):
        self.queue_urls = queue_urls
        self.deadline = deadline

    @gen.coroutine
    def schedule_queue_consumers(self):
        jobs = []
        for queue_url in self.queue_urls:
            queue_scheduler = QueueScheduler(queue_url, self.deadline)
            jobs.append(queue_scheduler.schedule())
        yield jobs


class QueueScheduler(object):

    def __init__(self, queue_url, deadline):
        self.queue_url = queue_url
        self.deadline = deadline
        self.batch_count = config.message_batch_count

    @gen.coroutine
    def _one_invoke_request(self, execution_count):
        uid = uuid.uuid4()
        invocation_event = event.new_control_event(
            self.queue_url, self.deadline, execution_count)
        logger.info("Invoke consumer with ID {} with event: {}".format(
            uid, invocation_event))
        result = yield gen.Task(
            lambda_invoke.call,
            FunctionName=config.consumer_arn,
            InvocationType='Event',
            Payload=invocation_event,
        )
        status = result['StatusCode']
        if status > 300:
            logger.warn("Invocation {} with error response: {}".format(
                uid, result))
        else:
            logger.info("Invocation {} succeeded.".format(uid))

    def _execution_counts(self, number_of_messages):
        number_of_batch = number_of_messages // self.batch_count

        if number_of_batch > config.max_concurrent_consumer_per_queue:
            concurrency = config.max_concurrent_consumer_per_queue
        elif number_of_batch != 0:
            concurrency = number_of_batch
        else:
            return []

        average = number_of_batch // concurrency
        left = number_of_batch % concurrency

        ret = [average] * concurrency
        for i in range(left):
            ret[i] += 1

        logger.info("Execution plan of queue {}: {}".format(
            self.queue_url, ret))
        return ret

    def _number_of_messages(self):
        result = sqs_number_of_visible_message.call(
            QueueUrl=self.queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )

        value = result['Attributes']['ApproximateNumberOfMessages']
        return int(value)

    @gen.coroutine
    def schedule(self):
        logger.info("Scheduling queue consumer: {}".format(self.queue_url))
        count = self._number_of_messages()
        execution_counts = self._execution_counts(count)
        jobs = []
        for count in execution_counts:
            jobs.append(self._one_invoke_request(count))
        yield jobs
        logger.info("Queue {} consumer scheduling complete.".format(
            self.queue_url))
