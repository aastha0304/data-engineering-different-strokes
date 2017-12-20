import { readFileSync } from 'fs';
import { resolve } from 'path';

const basePath = resolve(__dirname, '../../certs');
const readCryptoFile =
  filename => readFileSync(resolve(basePath, filename)).toString();
const config = {
  channelName: 'default',
  channelConfig: readFileSync(resolve(__dirname, '../../channel.tx')),
  chaincodeId: 'bcins',
  chaincodeVersion: 'v2',
  chaincodePath: 'bcins',
  orderer0: {
    hostname: 'orderer0',
    url: 'grpcs://orderer0:7050',
    pem: readCryptoFile('ordererOrg.pem')
  },
  reviewerOrg: {
    peer: {
      hostname: 'reviewer-peer',
      url: 'grpcs://reviewer-peer:7051',
      eventHubUrl: 'grpcs://reviewer-peer:7053',
      pem: readCryptoFile('reviewerOrg.pem')
    },
    ca: {
      hostname: 'reviewer-ca',
      url: 'https://reviewer-ca:7054',
      mspId: 'ReviewerOrgMSP'
    },
    admin: {
      key: readCryptoFile('Admin@reviewer-org-key.pem'),
      cert: readCryptoFile('Admin@reviewer-org-cert.pem')
    }
  },
  agencyOrg: {
    peer: {
      hostname: 'agency-peer',
      url: 'grpcs://agency-peer:7051',
      pem: readCryptoFile('agencyOrg.pem'),
      eventHubUrl: 'grpcs://agency-peer:7053',
    },
    ca: {
      hostname: 'agency-ca',
      url: 'https://agency-ca:7054',
      mspId: 'AgencyOrgMSP'
    },
    admin: {
      key: readCryptoFile('Admin@agency-org-key.pem'),
      cert: readCryptoFile('Admin@agency-org-cert.pem')
    }
  }
};

if (process.env.LOCALCONFIG) {
  config.orderer0.url = 'grpcs://localhost:7050';

  config.reviewerOrg.peer.url = 'grpcs://localhost:7051';
  config.agencyOrg.peer.url = 'grpcs://localhost:8051';

  config.reviewerOrg.peer.eventHubUrl = 'grpcs://localhost:7053';
  config.agencyOrg.peer.eventHubUrl = 'grpcs://localhost:8053';
  
  config.reviewerOrg.ca.url = 'https://localhost:7054';
  config.agencyOrg.ca.url = 'https://localhost:8054';
}

export default config;



