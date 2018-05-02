'use strict';

const test = require('ava');
const sinon = require('sinon');

const PutRecords = require('../../../lib/kinesis/put-records');

test('custom client', t => {
  t.plan(1);

  const client = {};
  const stream = new PutRecords('foo', { client });

  t.deepEqual(stream.kinesis, client);
});

test('writeRecords provides records and stream name', t => {
  t.plan(2);

  const stream = new PutRecords('foo');

  const promise = sinon.stub().resolves();

  stream.kinesis.putRecords = sinon.stub().returns({
    promise
  });

  return stream._writeRecords(['test'])
    .then(() => {
      const expected = {
          Records: ['test'],
          StreamName: 'foo'
      };
      t.deepEqual(stream.kinesis.putRecords.callCount, 1);
      t.deepEqual(stream.kinesis.putRecords.firstCall.args[0], expected);
    });
});

test('getFailedRecords identifies failed', t => {
  t.plan(1);
  
  const stream = new PutRecords('foo');

  const data = {
    FailedRecordCount: 1,
    Records: [{
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
    FailedRecordCount: 0,
    Records: [{
      ErrorCode: null
    }]
  };

  const failedRecords = stream._getFailedRecords(['test'], data);

  t.deepEqual(failedRecords, []);
});