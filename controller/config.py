import datetime


queue_urls = [
    'https://sqs.ap-northeast-1.amazonaws.com/164201395711/nobita-mailstat-test',
]

consumer_arn = 'arn:aws:lambda:ap-northeast-1:164201395711:function:nobita-mailstat-consumer-test:$LATEST'

message_batch_count = 100

output_table = 'nobita-mailstat-daily-test'

max_concurrent_consumer_per_queue = 1

job_interval = datetime.timedelta(minutes=30)
