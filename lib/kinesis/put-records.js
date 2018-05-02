'use strict';

'use strict';

const aws = require('aws-sdk');

const Writable = require('../writable');

module.exports = class KinesisPutRecords extends Writable {

  constructor(streamName, options = {}) {
    super(options);
    
    this.streamName = streamName;
    this.kinesis = options.client || new aws.Kinesis();
  }

  _writeRecords(records) {
    const params = {
      Records: records,
      StreamName: this.streamName
    };

    return this.kinesis.putRecords(params).promise();
  }

  _getFailedRecords(records, data) {
    if (data.FailedRecordCount) {
      const responses = data.Records;

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
