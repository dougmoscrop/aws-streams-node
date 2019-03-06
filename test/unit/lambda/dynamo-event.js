'use strict';

const test = require('ava');
const getStream = require('get-stream');

const DynamoEvent = require('../../../lib/lambda/dynamo-event');

test('throws when missing Records', async t => {
  t.plan(2);

  const err = t.throws(() =>  new DynamoEvent({}));

  t.is(err.message, 'event.Records was not an Array');
});

test('empty Records', async t => {
  t.plan(2);

  const stream = new DynamoEvent({ Records: []});
  
  const result = await getStream.array(stream);

  t.deepEqual(result, []);
  t.deepEqual(stream.position, 0);
});

test('record missing dynamo', async t => {
  t.plan(1);

  const stream = new DynamoEvent({ Records: [{}] });

  try {
    await getStream.array(stream);
    t.fail('should not reach here');
  } catch (err) {
    t.is(err.message, 'record missing dynamodb');
  }
});

test('record with valid data (OldImage)', async t => {
  t.plan(1);

  const dynamodb = { OldImage: { Message: { S: "Hello" } } };
  const stream = new DynamoEvent({ Records: [{ dynamodb }] });

  const result = await getStream.array(stream);
  t.deepEqual(result, [{ OldImage: { Message: "Hello" }}]);
});

test('record with valid data (NewImage)', async t => {
  t.plan(1);

  const dynamodb = { NewImage: { Message: { S: "Hello" } } };
  const stream = new DynamoEvent({ Records: [{ dynamodb }] });
  const result = await getStream.array(stream);

  t.deepEqual(result, [{ NewImage: { Message: "Hello" }}]);
});