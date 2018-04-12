'use strict';

const test = require('ava');
const aws = require('aws-sdk-mock');
const getStream = require('get-stream');

const deferred = require('../_deferred');
const Stream = require('../../lib/sqs/receive-message');

test.beforeEach(t => {
  t.context.sqsReceiveMessage = aws.mock('SQS', 'receiveMessage', (params, cb) => {
    t.context.receiveMessage(params, cb);
  });
  t.context.completed = deferred();
});

test.afterEach(() => {
  aws.restore();
});

test.serial('works', t => {  
  const { sqsReceiveMessage, completed } = t.context;
  const stream = new Stream('foo');

  let amount = 0;
  t.context.receiveMessage = (params, cb) => {
    if (amount < 3) {
      amount++
      cb(null, { Messages: [{ foo: 'bar' }, { foo: 'bar' }] });
    } else {  
      stream.stop();  

      setTimeout(() => {
        cb(null, { Messages: [] });
        completed.resolve();
      }, 100);
    }
  }

  return getStream.array(stream)
      .then(arr => {
        const expected = Array(6).fill({ foo: 'bar' });
        t.deepEqual(arr, expected);
        t.deepEqual(sqsReceiveMessage.stub.callCount, 4);
      })
      .then(() => completed);
});

test.serial('receive data after stop - flush 100', t => {
  const { sqsReceiveMessage, completed } = t.context;
  const stream = new Stream('foo', { flush: 100 });

  let amount = 0;
  t.context.receiveMessage = (params, cb) => {
    if (amount < 3) {
      amount++
      cb(null, { Messages: [{ foo: 'bar' }, { foo: 'bar' }] });
    } else {
      stream.stop();
      
      setTimeout(() => {
        cb(null, { Messages: [{ foo: 'bar' }, { foo: 'bar' }] });
        completed.resolve();
      }, 150);
    }
  };

  return getStream.array(stream)
      .then(arr => {
        const expected = Array(6).fill({ foo: 'bar' });
        t.deepEqual(arr, expected);
        t.deepEqual(sqsReceiveMessage.stub.callCount, 4);
      })
      .then(() => completed);
});

test.serial('stop on empty', t => {
  const { sqsReceiveMessage, completed } = t.context;
  const stream = new Stream('foo', { stopOnEmpty: true });

  let amount = 0;
  t.context.receiveMessage = (params, cb) => {
    if (amount < 2) {
      amount++
      cb(null, { Messages: [{ foo: 'bar' }, { foo: 'bar' }] });
    } else {
      cb(null, { Messages: [] });
      completed.resolve();
    }
  };

  return getStream.array(stream)
      .then(arr => {
        const expected = Array(4).fill({ foo: 'bar' });
        t.deepEqual(arr, expected);
        t.deepEqual(sqsReceiveMessage.stub.callCount, 3);
      })
      .then(() => completed);
});

test.serial('stop called multiple times', t => {
  const { sqsReceiveMessage, completed } = t.context;
  const stream = new Stream('foo', { stopOnEmpty: true });

  let amount = 0;
  t.context.receiveMessage = (params, cb) => {
    if (amount < 1) {
      amount++
      cb(null, { Messages: [{ foo: 'bar' }, { foo: 'bar' }] });
    } else {
      stream.stop();
      stream.stop();
      cb();
      completed.resolve();
    }
  };

  return getStream.array(stream)
      .then(arr => {
        const expected = Array(2).fill({ foo: 'bar' });
        t.deepEqual(arr, expected);
        t.deepEqual(sqsReceiveMessage.stub.callCount, 2);
      })
      .then(() => completed);
});

test.serial('works (receive data after stop - no flush)', t => {
  const { sqsReceiveMessage, completed } = t.context;
  const stream = new Stream('foo');
  
  let amount = 0;
  t.context.receiveMessage = (params, cb) => {
    if (amount < 3) {
      amount++
      cb(null, { Messages: [{ foo: 'bar' }, { foo: 'bar' }] });
    } else {
      stream.stop();
      
      setTimeout(() => {
        cb(null, { Messages: [{ foo: 'bar' }, { foo: 'bar' }] });
        completed.resolve();
      }, 0);
    }
  };

  return getStream.array(stream)
      .then(arr => {
        const expected = Array(6).fill({ foo: 'bar' });
        t.deepEqual(arr, expected);
        t.deepEqual(sqsReceiveMessage.stub.callCount, 4);
      })
      .then(() => completed);
});

test.serial('works (receive data after stop - flush true)', t => {
  const { sqsReceiveMessage, completed } = t.context;
  const stream = new Stream('foo', { flush: true });
  
  let amount = 0;
  t.context.receiveMessage = (params, cb) => {
    if (amount < 3) {
      amount++
      cb(null, { Messages: [{ foo: 'bar' }, { foo: 'bar' }] });
    } else {
      stream.stop();
      
      setTimeout(() => {
        cb(null, { Messages: [{ foo: 'bar' }, { foo: 'bar' }] });
        completed.resolve();
      }, 0);
    }
  };

  return getStream.array(stream)
      .then(arr => {
        const expected = Array(8).fill({ foo: 'bar' });
        t.deepEqual(arr, expected);
        t.deepEqual(sqsReceiveMessage.stub.callCount, 4);
      });
});

test.serial('errors when SQS response is missing Messages', t => {
  const { sqsReceiveMessage, completed } = t.context;
  const stream = new Stream('foo');

  t.context.receiveMessage = (params, cb) => {
    cb(null, {});
    completed.resolve();
  };

  return getStream.array(stream)
      .then(() => {
        t.fail('should not get here')
      })
      .catch(e => {
        t.deepEqual(e.message, 'Invalid response from SQS: data.Messages not an Array');
        t.deepEqual(sqsReceiveMessage.stub.callCount, 1);
      })
      .then(() => completed);
});

test.serial('retry on service error', t => {
  const { sqsReceiveMessage, completed } = t.context;
  const stream = new Stream('foo');

  let amount = 0;
  t.context.receiveMessage = (params, cb) => {
    if (amount < 1) {
      amount++
      const error = new Error();
      error.statusCode = 500;
      cb(error);
    } else {
      stream.stop();
      cb(null, { Messages: Array(1).fill({}) });
      completed.resolve();
    }
  };

  return getStream.array(stream)
      .then(() => {
        t.deepEqual(sqsReceiveMessage.stub.callCount, 2);
      })
      .then(() => completed);
});