'use strict';

const kinesis = require('./lib/kinesis');
const firehose = require('./lib/firehose');
const sqs = require('./lib/sqs');
const dynamodb = require('./lib/dynamodb');

module.exports = {
  kinesis,
  firehose,
  sqs,
  dynamodb
};