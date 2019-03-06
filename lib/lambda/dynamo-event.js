'use strict';

const { Readable } = require('stream');

const AWS = require('aws-sdk');

const { Converter } = AWS.DynamoDB;

module.exports = class LambdaDynamoEvent extends Readable {
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

        const { dynamodb } = this.records[this.position];

        if (dynamodb) {
          if (dynamodb.OldImage) {
            dynamodb.OldImage = Converter.unmarshall(dynamodb.OldImage);
          }

          if (dynamodb.NewImage) {
            dynamodb.NewImage = Converter.unmarshall(dynamodb.NewImage);
          }

          this.push(dynamodb);
          continue;
        }
        
        throw new Error('record missing dynamodb')
      }
    } catch (e) {
      this.emit('error', e);
    }
  }
}