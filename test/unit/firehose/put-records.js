'use strict';

const test = require('ava');
const sinon = require('sinon');

const PutRecords = require('../../../lib/firehose/put-records');

test('custom client', t => {
  t.plan(1);

  const client = {};
  const stream = new PutRecords('foo', { client });

  t.deepEqual(stream.firehose, client);
});

test('writeRecords provides records and stream name', t => {
  t.plan(2);

  const stream = new PutRecords('foo');

  const promise = sinon.stub().resolves();

  stream.firehose.putRecordBatch = sinon.stub().returns({
    promise
  });

  return stream._writeRecords(['test'])
    .then(() => {
      const expected = {
          Records: ['test'],
          DeliveryStreamName: 'foo'
      };
      t.deepEqual(stream.firehose.putRecordBatch.callCount, 1);
      t.deepEqual(stream.firehose.putRecordBatch.firstCall.args[0], expected);
    });
});

test('getFailedRecords identifies failed', t => {
  t.plan(1);

  const stream = new PutRecords('foo');

  const data = {
    FailedPutCount: 1,
    RequestResponses: [{
      ErrorCode: 'test',
      ErrorMessage: 'testing'
    }, {
      ErrorCode: null
    }]
  };

  const failedRecords = stream._getFailedRecords(['test1', 'test2'], data);

  t.deepEqual(failedRecords, ['test1']);
});

test('getFailedRecords returns empty', t => {
  t.plan(1);

  const stream = new PutRecords('foo');

  const data = {
    FailedPutCount: 0,
    RequestResponses: [{
      ErrorCode: null
    }]
  };

  const failedRecords = stream._getFailedRecords(['test'], data);

  t.deepEqual(failedRecords, []);
});