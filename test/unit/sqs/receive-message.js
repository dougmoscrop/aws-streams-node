'use strict';

const test = require('ava');
const sinon = require('sinon');

const ReceiveMessage = require('../../../lib/sqs/receive-message');

test('getRecords - Messages [] is missing', t => {
  t.plan(2);

  const stream = new ReceiveMessage('foo');

  const promise = sinon.stub().callsFake(() => {
    stream.stop();
    return Promise.resolve({});
  });

  stream.sqs.receiveMessage = sinon.stub().returns({
    promise
  });
  
  return stream._getRecords()
    .then(() => {
      t.deepEqual(promise.callCount, 1);
      t.deepEqual(stream.sqs.receiveMessage.callCount, 1);
    });
});

test('getRecords - passes through receiveParams', t => {
  t.plan(3);

  const stream = new ReceiveMessage('foo', {
    receiveParams: {
      VisibilityTimeout: 30,
      WaitTimeSeconds: 60
    }
  });

  const promise = sinon.stub().callsFake(() => {
    stream.stop();
    return Promise.resolve({ Messages: [] });
  });

  stream.sqs.receiveMessage = sinon.stub().returns({
    promise
  });
  
  return stream._getRecords()
    .then(() => {
      const expected = { QueueUrl: 'foo', VisibilityTimeout: 30, WaitTimeSeconds: 60 };

      t.deepEqual(promise.callCount, 1);
      t.deepEqual(stream.sqs.receiveMessage.callCount, 1);
      t.deepEqual(stream.sqs.receiveMessage.firstCall.args[0], expected);
    });
});