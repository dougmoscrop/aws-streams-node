'use strict';

const stream = require('stream');

const backoff = require('./backoff');

module.exports = class AwsWritable extends stream.Writable {
  
  constructor(options) {
    super({ objectMode: true });
      
    const { retryDelay = 200, retryLimit = 3 } = options;

    this.backoff = backoff(retryDelay, retryLimit);
  }

  _write(chunk, encoding, callback) {
    const records = [].concat(chunk);

    this._writeWithRetry(records)
      .then(() => callback())
      .catch(e => callback(e));
  }

  _writeWithRetry(records, attempt = 0) {
    return this._writeRecords(records)
      .then(response => {
        const failedRecords = this._getFailedRecords(records, response);

        if (failedRecords.length) {
          return this.backoff(attempt)
            .then(() => this._writeWithRetry(failedRecords, attempt + 1));
        }
      })
      .catch(e => {
        if (e.statusCode >= 500) {
          return this.backoff(attempt)
            .then(() => this._writeWithRetry(records, attempt + 1));
        }
        throw e;
      });
  }

};