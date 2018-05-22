'use strict';

const { Readable } = require('stream');

const backoff = require('./backoff');

const STOPPED = Symbol();
const FINISHED = Symbol();

module.exports = class ServiceReadable extends Readable {

  constructor(options = {}) {
    const {
      highWaterMark = 50,
      retryDelay = 200,
      retryLimit = 3,
      stopOnEmpty = false,
      flush = true,
      concurrency = 1
    } = options;

    super({ objectMode: true, highWaterMark });

    this.backoff = backoff(retryDelay, retryLimit);
    this.pending = new Set();
    
    Object.assign(this, { stopOnEmpty, flush, concurrency });
  }

  _read() {
    if (this[STOPPED]) {
      return;
    }

    this.wanted = true;
    this._maybeReceive();
  }

  _maybeReceive() {
    if (this.wanted && this.pending.size < this.concurrency) {
      this._receive();
    } 
  }

  _receive(attempt = 0) {
    const request = this._getRecords()
      .then(messages => {
        if (this[FINISHED] || (this[STOPPED] && !this.flush)) {
          return;
        }

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
      });

      if (attempt === 0) {
        this.pending.add(request);
        
        request.then(() => {
          this.pending.delete(request);
  
          if (this[STOPPED]) {
            return;
          }

          this._maybeReceive();
        });
      }
  }

  stop() {
    if (this[STOPPED]) {
      return;
    }

    this[STOPPED] = true;

    Promise.resolve()
      .then(() => {
        if (this.flush) {
          const wait = [
            Promise.all(Array.from(this.pending.values()))
          ];
    
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
      })
      .catch(e => {
        this.emit('error', e);
      });
  }

};
