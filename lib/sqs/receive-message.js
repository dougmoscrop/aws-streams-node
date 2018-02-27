'use strict';

const stream = require('stream');

const aws = require('aws-sdk');

const backoff = require('../backoff');

const STOPPED = Symbol();
const FINISHED = Symbol();

module.exports = class SqsReceiveMessage extends stream.Readable {

  constructor(queueUrl, options = {}) {
    super({ objectMode: true });

    const { retryDelay = 200, retryLimit = 3 } = options;
    const { stopOnEmpty = false, flush = false } = options;
    const { maxNumberOfMessages = 10, waitTimeSeconds = 0 } = options;

    Object.assign(this, { queueUrl, stopOnEmpty, flush, waitTimeSeconds, maxNumberOfMessages });

    this.backoff = backoff(retryDelay, retryLimit);
    this.sqs = new aws.SQS(options.sqs);
    this.wanted = 0;
  }

  stop() {
    if (this[STOPPED]) {
      return;
    }

    this[STOPPED] = true;

    Promise.resolve()
      .then(() => {
        if (this.flush) {
          const wait = [this.pending];
    
          if (Number.isInteger(this.flush)) {
            wait.push(new Promise(resolve => {
              setTimeout(resolve, this.flush);
            }));
          }

          return Promise.race(wait);
        }
      })
      .then(() => {
        this[FINISHED] = true;
        this.push(null);
      });
  }

  _read(amount) {
    if (this[STOPPED]) {
      return;
    }

    this.wanted = Math.min(this.wanted + amount, 20);

    if (this.pending) {
      return;
    }
    
    this.pending = this._receive();
  }

  _receive(attempt = 0) {
    const limit = Math.min(this.wanted, this.maxNumberOfMessages);

    const params = {
      QueueUrl: this.queueUrl,
      MaxNumberOfMessages: limit,
      WaitTimeSeconds: this.waitTimeSeconds
    };

    return this.sqs.receiveMessage(params)
      .promise()
      .then(data => {
        if (this[FINISHED] || (this[STOPPED] && !this.flush)) {
          return;
        }

        if (Array.isArray(data.Messages)) {
          if (data.Messages.length > limit) {
            throw new Error('SQS did not respect MaxNumberOfMessages');
          }

          if (data.Messages.length === 0 && this.stopOnEmpty) {
            this.stop();
            return;
          }

          for (let message of data.Messages) {
            this.push(message);
            this.wanted--;
          }
        } else {
          throw new Error('Invalid response from SQS: data.Messages not an array');
        }
      })
      .catch(e => {
        if (e.statusCode >= 500) {
          return this.backoff(attempt)
            .then(() => this._receive(attempt + 1));
        }

        this.emit('error', e);
        this.stop();
      })
      .then(() => {
        if (this[STOPPED]) {
          return;
        }
        
        this.pending = this.wanted ? this._receive() : null;
      });
  }

};