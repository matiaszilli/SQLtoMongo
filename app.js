
const { fork } = require('child_process');
let MsSqlClient   = require('mssql');


// Params
let config = require('./config/config');
const logger = require('./config/logger');
let sqlServerParams = require('./config/sqlserver');

// Libs
let getTables = require('./utils/getTables');

// Utils
let splitUp = require('./utils/splitUp');



(async function (){

    // get number of workers to create
    let nWorkers = config.nWorkers;

    // connect to sql server
    let sqlConn;
    let sqlUrl = `mssql://${sqlServerParams.user}:${sqlServerParams.password}@${sqlServerParams.host}:${sqlServerParams.port}/${sqlServerParams.database}`;
    try {
        // establish sql connection
        sqlConn = await MsSqlClient.connect(sqlUrl);
    } catch (err) {
        logger.error(err);
    }

    // get array of table names
    logger.info("Getting tables");
    let tables = await getTables(sqlConn, sqlServerParams.database);
    logger.info("Getting tables finished")

    // split tables into "workers" chunks
    let tables_splitted = splitUp(tables, nWorkers);

    // create "nWorkers" processes and send the tables to process to each one
    let child_count = 0;
    for(let j = 0 ; j < nWorkers ; j++) {
        
        // fork process
        const compute = fork('compute.js');
        // send list of tables to process by created process (compute)
        compute.send(tables_splitted[j]);
        // when process finishes
        compute.on('message', (msj) => {
            child_count++;
            logger.info("Worker " + child_count + " of " + nWorkers + " finished")
        });

    }

})();

