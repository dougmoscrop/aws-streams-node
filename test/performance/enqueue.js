'use strict';

const { SQS } = require('aws-sdk');
const Bluebird = require('bluebird');
const chunk = require('chunk');
const https = require('https');

const QueueUrl = process.env.QUEUE_URL;
const concurrency = Number(process.env.CONCURRENCY);

module.exports.handler = (event, context, callback) => {
  const amount = 4000;
  const start = new Date();

  write(amount)
    .then(() => {
      const end = new Date();
      const duration = end - start;
      const rps = amount / (duration / 1000);

      callback(null, rps);
    })
    .catch(e => callback(e));
};

function write(amount) {
  const sqs = new SQS({
    httpOptions: {
      agent: new https.Agent({ keepAlive: true })
    }
  });
  
  const arr = Array(amount).fill();

  return Bluebird.map(chunk(Object.keys(arr), 10), ids => {
    const entries = ids.map(id => {
      return {
        Id: id,
        MessageBody: 'test'
      };
    });

    const params = {
      Entries: entries,
      QueueUrl
    };

    return sqs.sendMessageBatch(params).promise();
  }, { concurrency });
}
/*
async function write(limit) {
  const sqs = new SQS({
    httpOptions: {
      agent: new https.Agent({ keepAlive: true })
    }
  });

  const arr = Object.keys(Array(10).fill());

  for (let i = 0; i < limit; i += 10) {
    const entries = (arr.map(v => {
      return {
        Id: i + v,
        MessageBody: 'test'
      }
    }));

    const params = {
      Entries: entries,
      QueueUrl
    };

    await sqs.sendMessageBatch(params).promise();
  }
}
*/