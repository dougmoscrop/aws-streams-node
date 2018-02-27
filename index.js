'use strict';

const kinesis = require('./lib/kinesis');
const firehose = require('./lib/firehose');
const sqs = require('./lib/sqs');

module.exports = {
  kinesis,
  firehose,
  sqs
};