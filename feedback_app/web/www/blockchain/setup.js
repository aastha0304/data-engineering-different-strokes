'use strict';

import config from './config';
import { OrganizationClient } from './utils';
import http from 'http';
import url from 'url';

let status = 'down';
let statusChangedCallbacks = [];

// Setup clients per organization
const reviewerClient = new OrganizationClient(
  config.channelName,
  config.orderer0,
  config.reviewerOrg.peer,
  config.reviewerOrg.ca,
  config.reviewerOrg.admin
);
const agencyClient = new OrganizationClient(
  config.channelName,
  config.orderer0,
  config.agencyOrg.peer,
  config.agencyOrg.ca,
  config.agencyOrg.admin
);
function setStatus(s) {
  status = s;

  setTimeout(() => {
    statusChangedCallbacks
      .filter(f => typeof f === 'function')
      .forEach(f => f(s));
  }, 1000);
}

export function subscribeStatus(cb) {
  if (typeof cb === 'function') {
    statusChangedCallbacks.push(cb);
  }
}

export function getStatus() {
  return status;
}

export function isReady() {
  return status === 'ready';
}

function getAdminOrgs() {
  return Promise.all([
    reviewerClient.getOrgAdmin(),
    agencyClient.getOrgAdmin()
  ]);
}

(async () => {
  // Login
  try {
    await Promise.all([
      reviewerClient.login(),
      agencyClient.login()
    ]);
  } catch (e) {
    console.log('Fatal error logging into blockchain organization clients!');
    console.log(e);
    process.exit(-1);
  }

  reviewerClient.initEventHubs();
  agencyClient.initEventHubs();

  // Bootstrap blockchain network
  try {
    await getAdminOrgs();
    if (!(await reviewerClient.checkChannelMembership())) {
      console.log('Default channel not found, attempting creation...');
      const createChannelResponse =
        await reviewerClient.createChannel(config.channelConfig);
      if (createChannelResponse.status === 'SUCCESS') {
        console.log('Successfully created a new default channel.');
        console.log('Joining peers to the default channel.');
        await Promise.all([
          reviewerClient.joinChannel(),
          agencyClient.joinChannel()
        ]);
        // Wait for 10s for the peers to join the newly created channel
        await new Promise(resolve => {
          setTimeout(resolve, 10000);
        });
      }
    }
  } catch (e) {
    console.log('Fatal error bootstrapping the blockchain network!');
    console.log(e);
    process.exit(-1);
  }

  // Initialize network
  try {
    await Promise.all([
      reviewerClient.initialize(),
      agencyClient.initialize()
    ]);
  } catch (e) {
    console.log('Fatal error initializing blockchain organization clients!');
    console.log(e);
    process.exit(-1);
  }
  
  // Install chaincode on all peers
  let installedOnReviewerOrg, installedOnAgencyOrg;
  
  try {
    await getAdminOrgs();
    installedOnReviewerOrg = await reviewerClient.checkInstalled(
      config.chaincodeId, config.chaincodeVersion, config.chaincodePath);
    installedOnAgencyOrg = await agencyClient.checkInstalled(
      config.chaincodeId, config.chaincodeVersion, config.chaincodePath);
  } catch (e) {
    console.log('Fatal error getting installation status of the chaincode!');
    console.log(e);
    process.exit(-1);
  }
  
  if (!(installedOnReviewerOrg && installedOnAgencyOrg)) {
    console.log('Chaincode is not installed, attempting installation...');
    // Pull chaincode environment base image
    try {
      await getAdminOrgs();
      const socketPath = process.env.DOCKER_SOCKET_PATH ||
      (process.platform === 'win32' ? '//./pipe/docker_engine' : '/var/run/docker.sock');
      const ccenvImage = process.env.DOCKER_CCENV_IMAGE ||
        'hyperledger/fabric-ccenv:x86_64-1.0.2';
      const listOpts = { socketPath, method: 'GET', path: '/images/json' };
      const pullOpts = {
        socketPath, method: 'POST',
        path: url.format({ pathname: '/images/create', query: { fromImage: ccenvImage } })
      };

      const images = await new Promise((resolve, reject) => {
        const req = http.request(listOpts, (response) => {
          let data = '';
          response.setEncoding('utf-8');
          response.on('data', chunk => { data += chunk; });
          response.on('end', () => { resolve(JSON.parse(data)); });
        });
        req.on('error', reject); req.end();
      });

      const imageExists = images.some(
        i => i.RepoTags && i.RepoTags.some(tag => tag === ccenvImage));
      if (!imageExists) {
        console.log('Base container image not present, pulling from Docker Hub...');
        await new Promise((resolve, reject) => {
          const req = http.request(pullOpts, (response) => {
            response.on('data', () => { });
            response.on('end', () => { resolve(); });
          });
          req.on('error', reject); req.end();
        });
        console.log('Base container image downloaded.');
      } else {
        console.log('Base container image present.');
      }
    } catch (e) {
      console.log('Fatal error pulling docker images.');
      console.log(e);
      process.exit(-1);
    }
    // Install chaincode
    const installationPromises = [
      reviewerClient.install(
        config.chaincodeId, config.chaincodeVersion, config.chaincodePath),
      agencyClient.install(
	config.chaincodeId, config.chaincodeVersion, config.chaincodePath)
    ];
    try {
      await Promise.all(installationPromises);
      await new Promise(resolve => {   setTimeout(resolve, 10000); });
      console.log('Successfully installed chaincode on the default channel.');
    } catch (e) {
      console.log('Fatal error installing chaincode on the default channel!');
      console.log(e);
      process.exit(-1);
    }
    
    // Instantiate chaincode on all peers
    // Instantiating the chaincode on a single peer should be enough (for now)
    try {
      // Initial contract types
      await reviewerClient.instantiate(config.chaincodeId,
	config.chaincodeVersion);
      console.log('Successfully instantiated chaincode on all peers.');
      setStatus('ready');
    } catch (e) {
      console.log('Fatal error instantiating chaincode on some(all) peers!');
      console.log(e);
      process.exit(-1);
    }
  } else {
    console.log('Chaincode already installed on the blockchain network.');
    setStatus('ready');
  }
})();

// Export organization clients
export {
  reviewerClient,
  agencyClient
};
