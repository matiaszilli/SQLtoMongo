
let MsSqlClient   = require('mssql');
let MongoClient = require('mongodb').MongoClient;

// Params
let sqlServerParams = require('./config/sqlserver');
let mongoParams  = require('./config/mongodb');
const logger = require('./config/logger');

// Libs
let perTable = require('./lib/perTable');

let mongoConn, 
    sqlConn;

async function compute(tables){

    logger.info('Connecting to DBs');

    mongoUrl = `mongodb://${mongoParams.host}:${mongoParams.port}/${mongoParams.database}`;
    sqlUrl = `mssql://${sqlServerParams.user}:${sqlServerParams.password}@${sqlServerParams.host}:${sqlServerParams.port}/${sqlServerParams.database}`;
    try {
        // establish mongo connection
        mongoConn = await MongoClient.connect(mongoUrl, { useNewUrlParser: true });
        // establish sql connection
        sqlConn = await MsSqlClient.connect(sqlUrl);
    } catch (err) {
        logger.error(err);
    }
    logger.info("Connected to DBs");

    // process each table
    for(let i = 0 ; i < tables.length ; i++) {
        await perTable(tables[i], sqlConn, mongoConn);
    }
    process.send("Worker finish");
}   

// listen for messages incoming from master process
process.on('message', compute);
    

