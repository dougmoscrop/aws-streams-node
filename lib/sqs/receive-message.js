'use strict';

const aws = require('aws-sdk');

const Readable = require('../readable');

const defaults = {
  concurrency: 16
};

module.exports = class SqsReceiveMessage extends Readable {

  constructor(queueUrl, options = {}) {
    const settings = Object.assign({}, defaults, options);

    super(settings);

    const { receiveParams = {} } = options;

    Object.assign(receiveParams, { QueueUrl: queueUrl });
    Object.assign(this, { receiveParams });

    this.sqs = options.client || new aws.SQS();
  }

  _getRecords() {
    return this.sqs.receiveMessage(this.receiveParams)
      .promise()
      .then(data => {
        return data.Messages || [];
      });
  }

};
