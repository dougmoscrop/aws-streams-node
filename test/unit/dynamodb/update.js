'use strict';

const test = require('ava');
const sinon = require('sinon');

const Update = require('../../../lib/dynamodb/update');

test('custom client', t => {
  t.plan(1);

  const client = {};
  const stream = new Update({}, { client });

  t.deepEqual(stream.dynamodb, client);
});

test('writeRecords merges records with params', t => {
  t.plan(2);

  const stream = new Update({ TableName: 'foo' });

  const promise = sinon.stub().resolves();

  stream.dynamodb.update = sinon.stub().returns({
    promise
  });

  return stream._writeRecords([{ Key: 'bar' }])
    .then(() => {
      const expected = {
          TableName: 'foo',
          Key: 'bar'
      };
      t.deepEqual(stream.dynamodb.update.callCount, 1);
      t.deepEqual(stream.dynamodb.update.firstCall.args[0], expected);
    });
});