'use strict';

const { Readable } = require('stream');

module.exports = class LambdaKinesisEvent extends Readable {
  constructor(event) {
    super({ objectMode: true });

    if (Array.isArray(event.Records)) {
      this.records = event.Records;
      this.end = this.records.length;
      this.position = 0;
    } else {
      throw new Error('event.Records was not an Array');
    }
  }

  _read(amount) {
    try {
      for (let stop = this.position + amount; this.position < stop; this.position++) {
        if (this.position === this.end) {
          this.push(null);
          return;
        }

        const record = this.records[this.position];

        if (record.kinesis && record.kinesis.data) {
          const data = Buffer.from(record.kinesis.data, 'base64');
          this.push(data);
          continue;
        }
        
        throw new Error('record missing kinesis.data')
      }
    } catch (e) {
      this.emit('error', e);
    }
  }
}