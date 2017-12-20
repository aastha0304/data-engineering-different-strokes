'use strict';

import { Server } from 'http';
import express from 'express';
import socketIo from 'socket.io';
import configureExpress from './config/express';
import agencyRouter, { wsConfig as agencyWsConfig }
  from './routers/agency.router';
import reviewerRouter, { wsConfig as reviewerWsConfig }
  from './routers/reviewer.router';

const REVIEWER_ROOT_URL = '/reviewer';
const AGENCY_ROOT_URL = '/agency';

const app = express();
const httpServer = new Server(app);

// Setup web sockets
const io = socketIo(httpServer);
agencyWsConfig(io.of(AGENCY_ROOT_URL));
reviewerWsConfig(io.of(REVIEWER_ROOT_URL));

configureExpress(app);

app.get('/', (req, res) => {
  res.render('home', { homeActive: true });
});

// Setup routing
app.use(AGENCY_ROOT_URL, agencyRouter);
app.use(REVIEWER_ROOT_URL, reviewerRouter);

export default httpServer;
