'use strict';

const test = require('ava');
const aws = require('aws-sdk-mock');
const intoStream = require('into-stream');
const pump = require('pump-promise');

const Stream = require('../../lib/firehose/put-records');

test.beforeEach(t => {
  t.context.firehosePutRecordBatch = aws.mock('Firehose', 'putRecordBatch', (params, cb) => {
    t.context.putRecordBatch(params, cb);
  });
});

test.afterEach(() => {
  aws.restore('Firehose', 'putRecordBatch');
});

test.serial('works', t => {
  const { firehosePutRecordBatch } = t.context;
  const stream = new Stream('foo');

  t.context.putRecordBatch = (params, cb) => cb(null, {});

  return pump(
    intoStream.obj([{ Data: 'test1' }]),
    stream
  )
  .then(() => {
    t.true(firehosePutRecordBatch.stub.calledOnce);
  });
});

test.serial('retries on failedPutCount', t => {
  const { firehosePutRecordBatch } = t.context;
  const stream = new Stream('foo');

  let callCount = 0;
  t.context.putRecordBatch = (params, cb) => {
    if (callCount === 0) {
      callCount++;
      const res = {
        FailedPutCount: 1,
        RequestResponses: [{
          ErrorCode: 'test',
          ErrorMessage: 'testing'
        }, {
          ErrorCode: null
        }]
      };
      cb(null, res);
    } else {
      cb(null, {});
    }
  };

  return pump(
    intoStream.obj([[{ Data: 'test1' }, { Data: 'test2' }]]),
    stream
  )
  .then(() => {
    t.true(firehosePutRecordBatch.stub.calledTwice);
  });
});

test.serial('rejects on error', t => {
  const { firehosePutRecordBatch } = t.context;
  const stream = new Stream('foo');

  t.context.putRecordBatch = (params, cb) => {
    cb(new Error('test'));
  };

  return pump(
    intoStream.obj([{ Data: 'test1' }]),
    stream
  )
  .then(() => {
    t.fail('expected an error');
  })
  .catch(e => {
    t.true(firehosePutRecordBatch.stub.calledOnce);
    t.deepEqual(e.message, 'test');
  });
});

test.serial('rejects when retryCountExceeded', t => {
  const { firehosePutRecordBatch } = t.context;
  const stream = new Stream('foo', { retryLimit: 2, retryDelay: 50 });

  t.context.putRecordBatch = (params, cb) => {
      cb(null, {
        FailedPutCount: 1,
        RequestResponses: [{
          ErrorCode: 'test',
          ErrorMessage: 'testing'
        }, {
          ErrorCode: null
        }]
      });
  };

  return pump(
    intoStream.obj([[{ Data: 'test1' }, { Data: 'test2' }]]),
    stream
  )
  .then(() => {
    t.fail('expected an error');
  })
  .catch(e => {
    t.deepEqual(firehosePutRecordBatch.stub.callCount, 3);
    t.deepEqual(e, 'Retry limit exceeded');
  });
});

test.serial('retries on service error', t => {
  const { firehosePutRecordBatch } = t.context;
  const stream = new Stream('foo');

  let callCount = 0;
  t.context.putRecordBatch = (params, cb) => {
    if (callCount === 0) {
      callCount++;
      const err = new Error();
      err.statusCode = 500;
      cb(err);
    } else {
      const res = {
        FailedPutCount: 0,
        RequestResponses: [{
          ErrorCode: null
        }, {
          ErrorCode: null
        }]
      };
      cb(null, res);
    }
  };

  return pump(
    intoStream.obj([[{ Data: 'test1' }, { Data: 'test2' }]]),
    stream
  )
  .then(() => {
    t.true(firehosePutRecordBatch.stub.calledTwice);
  });
});