'use strict';

const aws = require('aws-sdk');

const Writable = require('../writable');

module.exports = class DynamoDBUpdate extends Writable {

  constructor(params, options = {}) {
    const settings = Object.assign({}, options, {
      concurrency: 16,
    });

    super(settings);

    Object.assign(this, { params });

    this.dynamodb = options.client || new aws.DynamoDB.DocumentClient();
  }

  _writeRecords(records) {
    return Promise.all(records.map(record => {
        const params = Object.assign({}, this.params, record)

        return this.dynamodb.update(params).promise();
    }));
  }

};
