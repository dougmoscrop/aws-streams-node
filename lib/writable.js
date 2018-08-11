'use strict';

const { Writable } = require('stream');

const backoff = require('./backoff');

module.exports = class ServiceWritable extends Writable {
  
  constructor(options = {}) {
    const {
      highWaterMark = 50,
      retryDelay = 200,
      retryLimit = 3,
      concurrency = 1,
    } = options;    

    super({ objectMode: true, highWaterMark });
    
    this.backoff = backoff(retryDelay, retryLimit);
    this.pending = new Set();

    Object.assign(this, { concurrency });
  }

  _write(chunk, enc, callback) {
    const records = [].concat(chunk);
    const limited = (this.pending.size + 1) >= this.concurrency;
    
    this._writeWithRetry(records)
      .then(() => {
        if (limited) {
          callback();
        }
      });
    
    if (limited) {
      return false;
    } else {
      setImmediate(callback);
    }
  }

  _writeWithRetry(records, attempt = 0) {
    const request = this._writeRecords(records)
      .then(response => {
        return this._getFailedRecords(records, response);
      })
      .catch(e => {
        if (e.statusCode >= 500) {
          return records;
        }
        throw e;
      })
      .then(failedRecords => {
        if (failedRecords.length) {
          return this.backoff(attempt)
            .then(() => this._writeWithRetry(failedRecords, attempt + 1));
        }
      })
      .catch(e => {
        this.emit('error', e);
      });
    
    if (attempt === 0) {
      this.pending.add(request);

      request.then(() => {
        this.pending.delete(request);
      });
    }

    return request;
  }

  _getFailedRecords() {
    return [];
  }

  emit(event) {
    if (event === 'finish') {
      this._flush()
        .then(() => {
          Writable.prototype.emit.call(this, 'finish');
        })
        .catch(err => {
          Writable.prototype.emit.call(this, 'error', err);
        });
    } else {
      Writable.prototype.emit.apply(this, arguments);
    }
  }

  _flush() {
    return Promise.all(this.pending.values());
  }

};