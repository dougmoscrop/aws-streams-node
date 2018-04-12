'use strict';

const aws = require('aws-sdk');

const AwsWritable = require('../writable');

module.exports = class FirehosePutRecords extends AwsWritable {

  constructor(deliveryStreamName, options = {}) {
    super(options);
    
    this.deliveryStreamName = deliveryStreamName;
    this.firehose = new aws.Firehose(options.firehose);
  }

  _writeRecords(records) {
    const params = {
      Records: records,
      DeliveryStreamName: this.deliveryStreamName
    };

    return this.firehose.putRecordBatch(params).promise();
  }

  _getFailedRecords(records, data) {
    if (data.FailedPutCount) {
      const responses = data.RequestResponses;

      return records.filter((record, index) => {
        const response = responses[index];

        if (response.ErrorCode) {
          return true;
        }
      });
    }

    return [];
  }

};
