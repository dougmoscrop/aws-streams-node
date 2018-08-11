'use strict';

const test = require('ava');
const sinon = require('sinon');

const Scan = require('../../../lib/dynamodb/scan');

test('must override stopOnEmpty and flush', t => {
  t.plan(2);

  const stream = new Scan({}, { stopOnEmpty: true, flush: false });

  t.deepEqual(stream.stopOnEmpty, false);
  t.deepEqual(stream.flush, true);
});

test('custom client', t => {
  t.plan(1);

  const client = {};
  const stream = new Scan({}, { client });

  t.deepEqual(stream.dynamodb, client);
});

test('getRecords - passes through params', t => {
  t.plan(3);

  const stream = new Scan({ TableName: 'foo', ProjectionExpression: '#foo, #bar', });

  const promise = sinon.stub().callsFake(() => {
    return Promise.resolve({ Items: [] });
  });

  stream.dynamodb.scan = sinon.stub().returns({
    promise
  });
  
  return stream._getRecords()
    .then(() => {
      const expected = { TableName: 'foo', ProjectionExpression: '#foo, #bar' };

      t.deepEqual(promise.callCount, 1);
      t.deepEqual(stream.dynamodb.scan.callCount, 1);
      t.deepEqual(stream.dynamodb.scan.firstCall.args[0], expected);
    });
});

test('getRecords - tracks LastEvaluatedKey', t => {
  t.plan(3);

  const stream = new Scan({ TableName: 'foo' });

  const promise = sinon.stub().callsFake(() => {
    return Promise.resolve({ Items: [], LastEvaluatedKey: 'asdf' });
  });

  stream.dynamodb.scan = sinon.stub().returns({
    promise
  });
  
  return stream._getRecords()
    .then(() => {
      t.deepEqual(promise.callCount, 1);
      t.deepEqual(stream.params.LastEvaluatedKey, 'asdf');
      t.deepEqual(stream.dynamodb.scan.callCount, 1);
    });
});

test('getRecords - returns items', t => {
  t.plan(1);

  const stream = new Scan({ TableName: 'foo' });

  const items = [{
      foo: 'bar',
    }, {
      asdf: 'qwerty'
    }
  ];

  const promise = sinon.stub().callsFake(() => {
    return Promise.resolve({ Items: items });
  });

  stream.dynamodb.scan = sinon.stub().returns({
    promise
  });
  
  return stream._getRecords()
    .then(records => {
      t.deepEqual(records, items);
    });
});