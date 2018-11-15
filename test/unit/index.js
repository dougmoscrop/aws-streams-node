'use strict';

const test = require('ava');

const index = require('../../');

test('exports stuff', t => {
  t.deepEqual(typeof index.kinesis, 'object');
  t.deepEqual(typeof index.firehose, 'object');
  t.deepEqual(typeof index.sqs, 'object');
  t.deepEqual(typeof index.dynamodb, 'object');
  t.deepEqual(typeof index.lambda, 'object');
});