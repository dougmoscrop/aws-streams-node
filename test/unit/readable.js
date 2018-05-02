'use strict';

const getStream = require('get-stream');
const test = require('ava');
const sinon = require('sinon');

const Readable = require('../../lib/readable');

test('errors when thrown', t => {
  t.plan(2);

  const stream = new Readable('foo');

  const err = new Error('test');
  const _getRecords = sinon.stub().rejects(err);

  Object.assign(stream, { _getRecords });

  return getStream.array(stream)
    .then(() => {
      t.fail('should not get here');
    })
    .catch(e => {
      t.deepEqual(e.message, 'test');
      t.deepEqual(_getRecords.callCount, 1);
    });
});

test('retry on service error', t => {
  t.plan(2);

  const stream = new Readable({ concurrency: 1 });

  const err = Object.assign(new Error(), { statusCode: 500 });

  const _getRecords = sinon.stub()
    .onCall(0).rejects(err)
    .callsFake(() => {
      stream.stop();
      return Promise.resolve(['test']);
    })

  Object.assign(stream, { _getRecords });

  return getStream.array(stream)
    .then(arr => {
      t.deepEqual(arr, ['test']);
      t.deepEqual(_getRecords.callCount, 2);
    });
});

test('error during flush', t => {
  t.plan(2);

  const stream = new Readable();

  const _getRecords = sinon.stub().callsFake(() => {
    stream.stop();
    stream.pending = new Set([new Promise((resolve, reject) => {
      setTimeout(() => {
        reject(new Error('test'));
      }, 25);
    })]);
    return Promise.resolve([]);
  });

  Object.assign(stream, { _getRecords });

  return getStream.array(stream)
    .then(() => {
      t.fail('expected an error');
    })
    .catch(e => {
      t.deepEqual(e.message, 'test');
      t.deepEqual(_getRecords.callCount, 1);
    });
});

test('works concurrently', t => {
  t.plan(3);

  const stream = new Readable({ concurrency: 3 });

  let concurrent = 0

  const response = [{ foo: 'bar' }];

  const _getRecords = sinon.stub()
    .callsFake(() => {
      return new Promise(resolve => {
        setTimeout(() => {
          concurrent = Math.max(concurrent, stream.pending.size);
          resolve(response);
        }, 50);
      });
    })
    .onCall(10).callsFake(() => {
      stream.stop();
      return Promise.resolve(response);
    });
  
  Object.assign(stream, { _getRecords });

  return getStream.array(stream)
    .then(arr => {
      t.true(_getRecords.callCount > 10);
      t.deepEqual(arr, Array(_getRecords.callCount).fill({ foo: 'bar' }));
      t.deepEqual(concurrent, 3);
    })
});

test('works concurrently - with retry', t => {
  t.plan(3);

  const stream = new Readable({ concurrency: 3 });

  let concurrent = 0

  const response = [{ foo: 'bar' }];

  const err = Object.assign(new Error(), { statusCode: 500 });

  const _getRecords = sinon.stub()
    .callsFake(() => {
      return new Promise(resolve => {
        setTimeout(() => {
          concurrent = Math.max(concurrent, stream.pending.size);
          resolve(response);
        }, 50);
      });
    })
    .onCall(0).rejects(err)
    .onCall(10).callsFake(() => {
      stream.stop();
      return Promise.resolve(response);
    });
  
  Object.assign(stream, { _getRecords });

  return getStream.array(stream)
    .then(arr => {
      t.true(_getRecords.callCount > 10);
      t.deepEqual(arr, Array(_getRecords.callCount - 1).fill({ foo: 'bar' }));
      t.deepEqual(concurrent, 3);
    })
});

test('receive data after stop - flush 100', t => {
  t.plan(2);

  const stream = new Readable({ flush: 100 });

  const response = [{ foo: 'bar' }];

  const _getRecords = sinon.stub()
    .resolves(response)
    .onCall(4).callsFake(() => {
      stream.stop();
      return new Promise(resolve => {
        setTimeout(() => {
          resolve(response);
        }, 50);
      });
    });
  
  Object.assign(stream, { _getRecords });

  return getStream.array(stream)
    .then(arr => {
      t.true(_getRecords.callCount > 4);
      t.deepEqual(arr, Array(_getRecords.callCount).fill({ foo: 'bar' }));
    });
});

test('stop on empty', t => {
  t.plan(2);
  
  const stream = new Readable({ stopOnEmpty: true, concurrency: 1 });

  const _getRecords = sinon.stub()
    .onCall(0).resolves([{ foo: 'bar' }])
    .resolves([]);

  Object.assign(stream, { _getRecords });

  return getStream.array(stream)
    .then(arr => {
      t.deepEqual(arr, [{ foo: 'bar' }]);
      t.deepEqual(_getRecords.callCount, 2);
    });
});

test('stop called multiple times', t => {
  t.plan(2);

  const stream = new Readable({ stopOnEmpty: true, concurrency: 1 });

  const _getRecords = sinon.stub()
    .onCall(0).resolves([{ foo: 'bar' }])
    .callsFake(() => {
      stream.stop();
      stream.stop();
      return Promise.resolve([]);
    });

  Object.assign(stream, { _getRecords });

  return getStream.array(stream)
    .then(arr => {
      t.deepEqual(arr, [{ foo: 'bar' }]);
      t.deepEqual(_getRecords.callCount, 2);
    });
});

test('works (receive data after stop - no flush)', t => {
  t.plan(2);
  
  const stream = new Readable({ flush: false });
  
  const response = [{ foo: 'bar' }];

  const _getRecords = sinon.stub()
    .resolves(response)
    .onCall(1).callsFake(() => {
      stream.stop();
      
      return new Promise(resolve => {
        setTimeout(() => {
          resolve([{ foo: 'bar' }]);
        }, 25);
      });
    });

  Object.assign(stream, { _getRecords });

  return getStream.array(stream)
    .then(arr => {
      t.deepEqual(arr, [{ foo: 'bar' }]);
      t.deepEqual(_getRecords.callCount, 2);
    });
});

test('works (receive data after stop - flush true)', t => {
  t.plan(2);

  const stream = new Readable({ flush: true });
  
  const response = [{ foo: 'bar' }];

  const _getRecords = sinon.stub()
    .resolves(response)
    .onCall(1).callsFake(() => {
      stream.stop();
      
      return new Promise(resolve => {
        setTimeout(() => {
          resolve([{ foo: 'bar' }]);
        }, 25);
      });
    });

  Object.assign(stream, { _getRecords });

  return getStream.array(stream)
    .then(arr => {
      t.deepEqual(_getRecords.callCount, 2);
      t.deepEqual(arr, Array(2).fill({ foo: 'bar' }));
    });
});