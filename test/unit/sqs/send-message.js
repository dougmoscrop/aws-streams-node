'use strict';

const test = require('ava');
const sinon = require('sinon');

const SendMessage = require('../../../lib/sqs/send-message');

test('writeRecords', t => {
  t.plan(3);

  const stream = new SendMessage('test');
  
  const promise = sinon.stub().resolves();

  stream.sqs.sendMessageBatch = sinon.stub().returns({
    promise
  });

  return stream._writeRecords([{ MessageBody: 'test' }])
    .then(() => {
      const expected = {
        QueueUrl: 'test',
        Entries: [{
          Id: '0',
          MessageBody: 'test'
        }]
      };
      t.deepEqual(stream.sqs.sendMessageBatch.callCount, 1);
      t.deepEqual(stream.sqs.sendMessageBatch.firstCall.args, [expected]);
      t.deepEqual(promise.callCount, 1);
    });
});

test('getFailedRecords - a failed record', t => {
  t.plan(1);

  const stream = new SendMessage('test');

  const failedRecords = stream._getFailedRecords(['test1', 'test2'], { Failed: [{ Id: 1 }] });

  t.deepEqual(failedRecords, ['test2']);
});

test('getFailedRecords - no failed records', t => {
  t.plan(1);

  const stream = new SendMessage('test');

  const failedRecords = stream._getFailedRecords(['test1', 'test2'], {});

  t.deepEqual(failedRecords, []);
});