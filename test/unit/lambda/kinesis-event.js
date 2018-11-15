'use strict';

const test = require('ava');
const getStream = require('get-stream');

const KinesisEvent = require('../../../lib/lambda/kinesis-event');

test('throws when missing Records', async t => {
  t.plan(2);

  const err = t.throws(() =>  new KinesisEvent({}));

  t.is(err.message, 'event.Records was not an Array');
});

test('empty Records', async t => {
  t.plan(2);

  const stream = new KinesisEvent({ Records: []});
  
  const result = await getStream.array(stream);

  t.deepEqual(result, []);
  t.deepEqual(stream.position, 0);
});

test('record missing kinesis', async t => {
  t.plan(1);

  const stream = new KinesisEvent({ Records: [{}] });

  try {
    await getStream.array(stream);
    t.fail('should not reach here');
  } catch (err) {
    t.is(err.message, 'record missing kinesis.data');
  }
});

test('record missing kinesis.data', async t => {
  t.plan(1);

  const stream = new KinesisEvent({ Records: [{ kinesis: {} }] });

  try {
    await getStream.array(stream);
    t.fail('should not reach here');
  } catch (err) {
    t.is(err.message, 'record missing kinesis.data');
  }
});

test('record with valid data', async t => {
  t.plan(1);

  const data = Buffer.from('testing');
  const stream = new KinesisEvent({ Records: [{ kinesis: { data: data.toString('base64') } }] });

  const result = await getStream.array(stream);
  t.deepEqual(result, [data]);
});