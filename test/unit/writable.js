'use strict';

const intoStream = require('into-stream');
const pump = require('pump-promise');
const test = require('ava');
const sinon = require('sinon');

const Writable = require('../../lib/writable');

test('rejects on error', t => {
  t.plan(2);

  const stream = new Writable();

  const _writeRecords = sinon.stub().rejects(new Error('test'));

  Object.assign(stream, { _writeRecords });
  
  return pump(
    intoStream.obj([{ PartitionKey: 'test', Data: 'test1' }]),
    stream
  )
  .then(() => {
    t.fail('expected an error');
  })
  .catch(e => {
    t.deepEqual(_writeRecords.callCount, 1);
    t.deepEqual(e.message, 'test');
  });
});

test('rejects when retryCountExceeded due to failed records', t => {
  t.plan(3);
  
  const stream = new Writable({ retryLimit: 2, retryDelay: 50 });

  const _writeRecords = sinon.stub().resolves();
  const _getFailedRecords = sinon.stub().resolves(['test1']);

  Object.assign(stream, { _writeRecords, _getFailedRecords });

  return pump(
    intoStream.obj([['test1', 'test2']]),
    stream
  )
  .then(() => {
    t.fail('expected an error');
  })
  .catch(e => {
    t.deepEqual(e, 'Retry limit exceeded');
    t.deepEqual(_writeRecords.callCount, 3);
    t.deepEqual(_getFailedRecords.callCount, 3);
  });
});

test('retries on service error', t => {
  t.plan(3);

  const stream = new Writable({ retryLimit: 2, retryDelay: 50 });

  const err = Object.assign(new Error(), { statusCode: 500 });

  const _writeRecords = sinon.stub()
    .onFirstCall().rejects(err)
    .onSecondCall().resolves({
      FailedPutCount: 0,
      RequestResponses: [{
        ErrorCode: null
      }, {
        ErrorCode: null
      }]
    });

  Object.assign(stream, { _writeRecords });

  return pump(
    intoStream.obj([[{ Data: 'test1' }, { Data: 'test2' }]]),
    stream
  )
  .then(() => {
    t.deepEqual(_writeRecords.callCount, 2);
    t.deepEqual(_writeRecords.firstCall.args[0], [{ Data: 'test1' }, { Data: 'test2' }]);
    t.deepEqual(_writeRecords.secondCall.args[0], [{ Data: 'test1' }, { Data: 'test2' }]);
  });
});

test('concurrency - works', t => {
  const stream = new Writable({ concurrency: 2, retryLimit: 2, retryDelay: 50 });

  let concurrent = 0;

  stream._writeRecords = sinon.stub().callsFake(() => {
    return new Promise(resolve => {
      setTimeout(() => {
        concurrent = Math.max(concurrent, stream.pending.size);
        resolve();
      }, 100);
    });
  });

  return pump(
    intoStream.obj(['test1', 'test2', 'test3', 'test4']),
    stream
  )
  .then(() => {
    t.deepEqual(stream._writeRecords.callCount, 4);
    t.deepEqual(concurrent, 2);
  });
});

test('concurrency - flush error', t => {
  t.plan(4);

  const stream = new Writable({ concurrency: 2 });

  const _writeRecords = sinon.stub().resolves();
  const _flush = sinon.stub().rejects(new Error('test'));

  Object.assign(stream, { _writeRecords, _flush });

  return pump(
    intoStream.obj(['test1']),
    stream
  )
  .then(() => {
    t.fail('expected an error');
  })
  .catch(e => {
    t.deepEqual(_writeRecords.callCount, 1);
    t.deepEqual(_writeRecords.firstCall.args[0], ['test1']);
    t.deepEqual(_flush.callCount, 1);
    t.deepEqual(e.message, 'test');
  });
});
