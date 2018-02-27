# aws-streams

This library offers stream-like interfaces to various AWS services.

Most streams will take care of batching but it is up to the user to ensure the batch size is appropriate. The same is true for any given limit on the size of an individual record.

All streams include, where appropriate, backoff and retry with jitter.

The goal of this library is to be minimal and essentially do the least amount of work required to handle the data so that the user can decide how best to process it (e.g. fan out to Lambda workers)

## Usage Example

```js
const { sqs } = require('aws-streams');

const receiveMessage = new sqs.ReceiveMessage('queue-url')
const processMessage = ...
const deleteMessage = new sqs.DeleteMessage('queue-url');

receiveMessage.pipe(processMessage).pipe(deleteMessage);
```