'use strict';

const test = require('ava');
const aws = require('aws-sdk-mock');
const intoStream = require('into-stream');
const pump = require('pump-promise');

const Stream = require('../../../lib/sqs/delete-message');

test.beforeEach(t => {
  t.context.sqsDeleteMessageBatch = aws.mock('SQS', 'deleteMessageBatch', (params, cb) => {
    t.context.deleteMessageBatch(params, cb);
  });
});

test.afterEach(() => {
  aws.restore('SQS', 'deleteMessageBatch');
});

test.serial('works', t => {
  const { sqsDeleteMessageBatch } = t.context;
  const stream = new Stream('foo');

  t.context.deleteMessageBatch = (params, cb) => {
    cb(null, {});
  };

  return pump(
    intoStream.obj([['123', '456']]),
    stream
  )
  .then(() => {
    t.true(sqsDeleteMessageBatch.stub.calledOnce);
  });
});

test.serial('rejects on error', t => {
  const { sqsDeleteMessageBatch } = t.context;
  const stream = new Stream('foo');

  t.context.deleteMessageBatch = (params, cb) => {
    cb(new Error('test'));
  };

  return pump(
    intoStream.obj(['123']),
    stream
  )
  .then(() => {
    t.fail('expected an error');
  })
  .catch(e => {
    t.true(sqsDeleteMessageBatch.stub.calledOnce);
    t.deepEqual(e.message, 'test');
  });
});

test.serial('rejects when retryCountExceeded', t => {
  const { sqsDeleteMessageBatch } = t.context;
  const stream = new Stream('foo', { retryLimit: 2, retryDelay: 50 });

  t.context.deleteMessageBatch = (params, cb) => {
    cb(null, {
      Failed: [{
        Id: 0,
        Code: 'test',
        Message: 'foo'
      }, {
        Id: 1,
        Code: 'test',
        Message: 'bar'
      }]
    });
  };

  return pump(
    intoStream.obj([['test1', 'test2']]),
    stream
  )
  .then(() => {
    t.fail('expected an error');
  })
  .catch(e => {
    t.deepEqual(e, 'Retry limit exceeded');
    t.deepEqual(sqsDeleteMessageBatch.stub.callCount, 3);
  });
});