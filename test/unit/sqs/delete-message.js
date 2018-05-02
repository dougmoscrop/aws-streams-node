'use strict';

const test = require('ava');
const sinon = require('sinon');

const DeleteMessage = require('../../../lib/sqs/delete-message');

test('writeRecords', t => {
  t.plan(3);

  const stream = new DeleteMessage('test');
  
  const promise = sinon.stub().resolves();

  stream.sqs.deleteMessageBatch = sinon.stub().returns({
    promise
  });

  return stream._writeRecords(['test'])
    .then(() => {
      const expected = {
        QueueUrl: 'test',
        Entries: [{
          Id: '0',
          ReceiptHandle: 'test'
        }]
      };
      t.deepEqual(stream.sqs.deleteMessageBatch.callCount, 1);
      t.deepEqual(stream.sqs.deleteMessageBatch.firstCall.args, [expected]);
      t.deepEqual(promise.callCount, 1);
    });
});

test('getFailedRecords - a failed record', t => {
  t.plan(1);

  const stream = new DeleteMessage('test');

  const failedRecords = stream._getFailedRecords(['test1', 'test2'], { Failed: [{ Id: 1 }] });

  t.deepEqual(failedRecords, ['test2']);
});

test('getFailedRecords - no failed records', t => {
  t.plan(1);

  const stream = new DeleteMessage('test');

  const failedRecords = stream._getFailedRecords(['test1', 'test2'], {});

  t.deepEqual(failedRecords, []);
});