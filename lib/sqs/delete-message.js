'use strict';

const aws = require('aws-sdk');

const Writable = require('../writable');

const defaults = {
  concurrency: 16
};

module.exports = class SqsDeleteMessage extends Writable {

  constructor(queueUrl, options = {}) {
    const settings = Object.assign({}, defaults, options);

    super(settings);

    this.queueUrl = queueUrl;
    this.sqs = options.client || new aws.SQS();
  }

  _writeRecords(records) {
    const params = {
      QueueUrl: this.queueUrl,
      Entries: records.map((record, index) => {
        return {
          Id: index.toString(),
          ReceiptHandle: record
        };
      })
    };

    return this.sqs.deleteMessageBatch(params).promise();
  }

  _getFailedRecords(records, data) {
    if (data.Failed && data.Failed.length) {
      return data.Failed.map(failed => {
        return records[failed.Id];
      });
    }

    return [];
  }
  
};
