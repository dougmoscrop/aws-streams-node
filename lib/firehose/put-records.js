'use strict';

const stream = require('stream');

const aws = require('aws-sdk');

const backoff = require('../backoff');

module.exports = class FirehosePutRecords extends stream.Writable {

  constructor(deliveryStreamName, options = {}) {
    super({ objectMode: true });
    
    const { retryDelay = 200, retryLimit = 3 } = options;

    this.deliveryStreamName = deliveryStreamName;
    this.backoff = backoff(retryDelay, retryLimit);
    this.firehose = new aws.Firehose(options.firehose);
  }

  _write(chunk, encoding, callback) {
    const records = [].concat(chunk);

    this._putRecords(records)
      .then(() => callback())
      .catch(e => callback(e));
  }

  _putRecords(records, attempt = 0) {
    const params = {
      Records: records,
      DeliveryStreamName: this.deliveryStreamName
    };

    return this.firehose
      .putRecordBatch(params)
      .promise()
      .then(data => {
        if (data.FailedPutCount) {
          const responses = data.RequestResponses;

          const failedRecords = records.filter((record, index) => {
            const response = responses[index];

            if (response.ErrorCode) {
              this.emit('failed', { attempt, index, record, response });
              return true;
            }
          });

          return this.backoff(attempt)
            .then(() => this._putRecords(failedRecords, attempt + 1));
        }
      })
      .catch(e => {
        if (e.statusCode >= 500) {
          return this.backoff(attempt)
            .then(() => this._putRecords(records, attempt + 1));
        }
        throw e;
      });
  }

};
