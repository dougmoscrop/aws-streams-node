'use strict';

const stream = require('stream');

const aws = require('aws-sdk');

const backoff = require('../backoff');

module.exports = class SqsDeleteMessage extends stream.Writable {

  constructor(queueUrl, options = {}) {
    super({ objectMode: true });

    const { retryDelay = 200, retryLimit = 3 } = options;

    Object.assign(this, { queueUrl });

    this.backoff = backoff(retryDelay, retryLimit);
    this.sqs = new aws.SQS(options.sqs);
  }

  _write(chunk, encoding, callback) {
    Promise.resolve()
      .then(() => {
        const batch = [].concat(chunk);
        return this._deleteMessageBatch(batch);
      })
      .then(() => callback())
      .catch(e => callback(e));
  }

  _deleteMessageBatch(batch, attempt = 0) {
    const params = {
      QueueUrl: this.queueUrl,
      Entries: batch.map((entry, index) => {
        return {
          Id: index.toString(),
          ReceiptHandle: entry
        };
      })
    };

    return this.sqs.deleteMessageBatch(params)
      .promise()
      .then(data => {
        if (data.Failed && data.Failed.length) {
          const failedRecords = data.Failed.map(failed => {
            const entry = batch[failed.Id];

            this.emit('failed', { attempt, entry, failed });

            return entry;
          });

          return this.backoff(attempt)
            .then(() => this._deleteMessageBatch(failedRecords, attempt + 1));
        }
      });
  }
  
};