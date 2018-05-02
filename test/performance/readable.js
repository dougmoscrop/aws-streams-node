'use strict';

const getStream = require('get-stream');
const sinon = require('sinon');
const test = require('ava');

const Readable = require('../../lib/readable');

test.only('basic perf test with 100ms latency, sustains 1000 req/sec', t => {
  const stream = new Readable({ concurrency: 16 });

  const response = Array(10).fill([{ foo: 'bar' }]);

  const _getRecords = sinon.stub()
    .callsFake(() => {
      return new Promise(resolve => {
        setTimeout(() => {
          resolve(response);
        }, 100);
      });
    })
    .onCall(200)
    .callsFake(() => {
      stream.stop();
      return Promise.resolve(response);
    });

  Object.assign(stream, { _getRecords });

  const start = new Date();
  return getStream.array(stream)
    .then(arr => {
      const end = new Date();
      const duration = end - start;
      const amount = arr.length;

      const rps = amount / (duration / 1000);

      t.true(rps > 1000);
    });
});