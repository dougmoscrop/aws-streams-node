# aws-streams

This library offers stream-like interfaces to various AWS services.

Most streams will take care of batching but it is up to the user to ensure the batch size is appropriate. The same is true for any given limit on the size of an individual record.

All streams include, where appropriate, backoff and retry with jitter.

The goal of this library is to be minimal and essentially do the least amount of work required to handle the data so that the user can decide how best to process it (e.g. fan out to Lambda workers)

## Usage

```js
const { service } = require('aws-streams');

new service.Method('...', options);
```

The first argument depends on the service - it might be the SQS Queue URL, or the Firehose Delivery Stream Name, etc. and the second argument is options, which is shown here with defaults:

```js
options: {
  client: undefined // custom client to use, or else new one is created from aws-sdk,
  retryLimit: 3,
  retryDelay: 200
}
```

### Example

This uses a custom-configured SQS client and longer retry delay, to simply empty an SQS queue:

```js
const aws = require('aws-sdk');
const { sqs } = require('aws-streams');
const stream = require('stream');

const client = new aws.SQS({ ... });
const retryDelay = 500;

const receiveMessage = new sqs.ReceiveMessage('queue-url', { client, retryDelay })
const processMessage = new stream.Transform({
  transform: (message, encoding, cb) => {
    cb(null, message.ReceiptHandle);
  }
});
const deleteMessage = new sqs.DeleteMessage('queue-url', { client, retryDelay });

receiveMessage.pipe(processMessage).pipe(deleteMessage);
```