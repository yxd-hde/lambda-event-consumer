# Event consumer

Event consumer does the following:

1. poll messages from the queue and do some statistics around them.
2. save the statistics into dynamodb
3. delete messages from the queue
