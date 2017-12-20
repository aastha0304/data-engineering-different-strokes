'use strict';

import config from './config';
import { wrapError } from './utils';
import { reviewerClient as client, isReady } from './setup';

export async function getReviews(status) {
  if (!isReady()) {
    return;
  }
  try {
    if (typeof status !== 'string') {
      status = undefined;
    }
    const reviews = await query('review_ls', { status });
    return reviews;
  } catch (e) {
    let errMessage;
    if (status) {
      errMessage = `Error getting reviews with status ${status}: ${e.message}`;
    } else {
      errMessage = `Error getting all reviews: ${e.message}`;
    }
    throw wrapError(errMessage, e);
  }
}

export async function fileReview(review) {
  if (!isReady()) {
    return;
  }
  try {
    const c = Object.assign({}, review);
    const successResult = await invoke('review_file', c);
    if (successResult) {
      throw new Error(successResult);
    }
    return c.uuid;
  } catch (e) {
    throw wrapError(`Error filing a new review: ${e.message}`, e);
  }
}

export function getBlocks(noOfLastBlocks) {
  return client.getBlocks(noOfLastBlocks);
}

export const on = client.on.bind(client);
export const once = client.once.bind(client);
export const addListener = client.addListener.bind(client);
export const prependListener = client.prependListener.bind(client);
export const removeListener = client.removeListener.bind(client);

function invoke(fcn, ...args) {
  return client.invoke(
    config.chaincodeId, config.chaincodeVersion, fcn, ...args);
}

function query(fcn, ...args) {
  return client.query(
    config.chaincodeId, config.chaincodeVersion, fcn, ...args);
}
