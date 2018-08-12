'use strict';

const aws = require('aws-sdk');

const Readable = require('../readable');

module.exports = class DynamoDBScan extends Readable {

  constructor(params, options = {}) {
    const settings = Object.assign({}, options, {
      concurrency: 1,
      flush: true,
      stopOnEmpty: false
    });

    super(settings);

    Object.assign(this, { params });

    this.dynamodb = options.client || new aws.DynamoDB.DocumentClient();
  }

  _getRecords() {
    return this.dynamodb.scan(this.params)
      .promise()
      .then(data => {
        if (data.LastEvaluatedKey) {
          this.params = Object.assign({}, this.params, {
            LastEvaluatedKey: data.LastEvaluatedKey
          });
        } else {
          this.stop();
        }
        return data.Items;
      });
  }

};
