import datetime


queue_urls = [
    'https://sqs.ap-northeast-1.amazonaws.com/164201395711/nobita-mailstat-test',
    'https://sqs.ap-northeast-1.amazonaws.com/164201395711/nobita-mailstat-test-0'
]

consumer_arn = 'arn:aws:lambda:ap-northeast-1:164201395711:function:nobita-mailstat-consumer-test:$LATEST'

message_batch_count = 500

output_table = 'nobita-mailstat-daily-test'

max_concurrent_consumer_per_queue = 2

job_interval = datetime.timedelta(minutes=30)
