# CloudKon

This project is a distributed task execution framework on Amazon EC2using the SQS. The project is a task execution framework (similar to [CloudKon](http://datasys.cs.iit.edu/publications/2014_CCGrid14_CloudKon.pdf)).
 
It uses EC2 to run the framework; the framework is separated into two components, client (e.g. a command line tool which submits tasks to SQS) and workers (e.g. which retrieve tasks from SQS and executes them). The SQS service is used to handle the queue of requests to load balance across multiple workers.

SQS does not guarantee that messages from SQS are delivered exactly once. It uses DynamoDB to keep track of what messages have been seen, so that if a duplicate is retrieved, it can be discarded.

### Tasks types
This project was to be used to be used to make performance tests over EC2, SQS and DynamoDB **not to acomplish complex tasks**, hence it only takes *sleep* tasks (e.g. sleep 100) and *animoto* tasks.

Animoto tasks consist on a list of images links that would be dowloaded and convert to an image slides video. This resulting video would be uploaded to S3. This tasks would give performance info about the processing capacity of the intance used.