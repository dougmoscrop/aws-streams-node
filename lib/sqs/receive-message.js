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
    const { stopOnEmpty = false, flush = false, receiveParams } = options;

    Object.assign(this, { queueUrl, stopOnEmpty, flush, receiveParams });

    this.backoff = backoff(retryDelay, retryLimit);
    this.sqs = options.client || new aws.SQS();
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

  _read() {
    if (this[STOPPED] || this.pending) {
      return;
    }
    
    this.wanted = true;
    this._receive();
  }

  _receive(attempt = 0) {
    const params = Object.assign({
      QueueUrl: this.queueUrl
    }, this.receiveParams);

    this.pending = this.sqs.receiveMessage(params)
      .promise()
      .then(data => {
        if (this[FINISHED] || (this[STOPPED] && !this.flush)) {
          return;
        }

        const messages = data.Messages || [];

        if (messages.length === 0 && this.stopOnEmpty) {
          this.stop();
          return;
        }

        for (let message of messages) {
          this.wanted = this.push(message);
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
        
        this.pending = null;

        if (this.wanted) {
          this._receive();
        }
      });
  }

};
