'use strict';

const test = require('ava');
const aws = require('aws-sdk-mock');
const intoStream = require('into-stream');
const pump = require('pump-promise');

const Stream = require('../../../lib/kinesis/put-records');

test.beforeEach(t => {
  t.context.kinesisPutRecords = aws.mock('Kinesis', 'putRecords', (params, cb) => {
    t.context.putRecords(params, cb);
  });
});

test.afterEach(() => {
  aws.restore('Kinesis', 'putRecords');
});

test.serial('works', t => {
  const { kinesisPutRecords } = t.context;
  const stream = new Stream('foo');

  t.context.putRecords = (params, cb) => {
    cb(null, {});
  };

  return pump(
    intoStream.obj([{ PartitionKey: 'test', Data: 'test1' }]),
    stream
  )
  .then(() => {
    t.true(kinesisPutRecords.stub.calledOnce);
  });
});

test.serial('retries on FailedRecordCount', t => {
  const { kinesisPutRecords } = t.context;
  const stream = new Stream('foo');

  let callCount = 0;
  t.context.putRecords = (params, cb) => {
    if (callCount === 0) {
      callCount++;
      const res = {
        FailedRecordCount: 1,
        Records: [{
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
    intoStream.obj([[{ PartitionKey: 'test', Data: 'test1' }, { PartitionKey: 'test2', Data: 'test2' }]]),
    stream
  )
  .then(() => {
    t.true(kinesisPutRecords.stub.calledTwice);
  });
});

test.serial('rejects on error', t => {
  const { kinesisPutRecords } = t.context;
  const stream = new Stream('foo');

  t.context.putRecords = (params, cb) => {
    cb(new Error('test'));
  };

  return pump(
    intoStream.obj([{ PartitionKey: 'test', Data: 'test1' }]),
    stream
  )
  .then(() => {
    t.fail('expected an error');
  })
  .catch(e => {
    t.true(kinesisPutRecords.stub.calledOnce);
    t.deepEqual(e.message, 'test');
  });
});

test.serial('rejects when retryCountExceeded', t => {
  const { kinesisPutRecords } = t.context;
  const stream = new Stream('foo', { retryLimit: 2, retryDelay: 50 });

  t.context.putRecords = (params, cb) => {
      cb(null, {
        FailedRecordCount: 1,
        Records: [{
          ErrorCode: 'test',
          ErrorMessage: 'testing'
        }, {
          ErrorCode: null
        }]
      });
  };

  return pump(
    intoStream.obj([{ PartitionKey: 'test', Data: 'test1' }]),
    stream
  )
  .then(() => {
    t.fail('expected an error');
  })
  .catch(e => {
    t.deepEqual(kinesisPutRecords.stub.callCount, 3);
    t.deepEqual(e, 'Retry limit exceeded');
  });
});

test.serial('retries on service error', t => {
  const { kinesisPutRecords } = t.context;
  const stream = new Stream('foo');
  
  let callCount = 0;
  t.context.putRecords = (params, cb) => {
    if (callCount === 0) {
      callCount++;
      const err = new Error();
      err.statusCode = 500;
      cb(err);
    } else {
      const res = {
        FailedRecordCount: 0,
        Records: [{
          ErrorCode: null
        }, {
          ErrorCode: null
        }]
      };
      cb(null, res);
    }
  };

  return pump(
    intoStream.obj([[{ PartitionKey: 'test', Data: 'test1' }, { PartitionKey: 'test2', Data: 'test2' }]]),
    stream
  )
  .then(() => {
    t.true(kinesisPutRecords.stub.calledTwice);
  });
});