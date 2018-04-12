'use strict';

const aws = require('aws-sdk');

const AwsWritable = require('../writable');

module.exports = class SqsDeleteMessage extends AwsWritable {

  constructor(queueUrl, options = {}) {
    super(options);

    this.queueUrl = queueUrl;
    this.sqs = new aws.SQS(options.sqs);
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