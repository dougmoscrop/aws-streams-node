'use strict';

const { Readable, Writable } = require('stream');

class ConcurrentWritable extends Writable {

  constructor() {
    super({ objectMode: true, highWaterMark: 4 });

    this.pending = new Set();
    this.concurrency = 4;
  }

  _write(chunk, enc, cb) {
    const request = new Promise(resolve => {
      setTimeout(() => {
        resolve();
      }, 250);
    });

    this.pending.add(request);

    const limited = this.pending.size >= this.concurrency;

    request.then(() => {
      if (limited) cb();
      this.pending.delete(request);
    });
    
    if (limited) {
      return false;
    } else {
      setImmediate(cb);
    }
  }

  emit(event) {
    if (event === 'finish') {
      this._flush(err => {
        if (err) {
          Writable.prototype.emit.call(this, 'error', err);
        } else {
          Writable.prototype.emit.call(this, 'finish');
        }
      });
    } else {
      Writable.prototype.emit.apply(this, arguments);
    }
  }

  _flush(cb) {
    Promise.all(this.pending.values())
      .then(() => {
        cb();
      })
      .catch(e => {
        cb(e);
      });
  }

}

let amount = 0;
const readable = new Readable({
  objectMode: true,
  highWaterMark: 10,
  read: function() {
    amount++;
    if (amount === 50) {
      this.push(null);
    }
  }
});

const writable = new ConcurrentWritable();

readable.pipe(writable);