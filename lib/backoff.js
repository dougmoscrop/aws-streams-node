'use strict';

module.exports = function backoff(delay, limit) {
  return attempt => {
    if (attempt < limit) {
      return new Promise(resolve => {
        const timeout = Math.round(Math.random() * delay * Math.pow(2, attempt));
        setTimeout(resolve, timeout);
      });
    }
    return Promise.reject(new Error('Retry limit exceeded'));
  }
};