
// Params
let mongoParams  = require('../config/mongodb');
const logger = require('../config/logger');

// Libs
let parseData = require('./parseData');

/**
 * Process each table of Sql Server Database.
 *
 * @param {String} tableName
 * @param {Object} sqlConn
 * @param {Object} mongoConn
 * @return 
 */

module.exports = function perTable(tableName, sqlConn, mongoConn) {
    return new Promise((resolve, reject) => {
        const NS_PER_SEC = 1e9;
        const MS_PER_NS = 1e-9;

        let db = mongoConn.db(mongoParams.database);
        let collection = db.collection(mongoParams.collection)
    
        var request = sqlConn.request()
        let start_select = process.hrtime(); // start timer
        // Querying MSSQL
        request.query("SELECT * FROM " + tableName, async (err, rows) => {
            if (err) {
                logger.error(err);
                reject(err);
                return;
            }
                
            let nrows = rows.recordsets[0].length; // get nrows of table
            if(nrows === 0) { // if the table is empty
                resolve(true);
                return;
            }
            time_select = process.hrtime(start_select); // end timer
            time_select_s = ((time_select[0] * NS_PER_SEC + time_select[1])  * MS_PER_NS).toFixed(2);
            logger.info(`  Querying table ${tableName} finished in ${time_select_s} rows: ${nrows} (rows/s): ${(nrows/time_select_s).toFixed()}`);

            let record_rows = rows.recordsets[0]; // mssql rows
            // Loop for rows in a given table
            for (let i = 0 ; i < record_rows.length ; i++) {
                let row = record_rows[i];
                // Parse Data for each row
                await parseData(row, tableName);
            }
            let start_insert = process.hrtime(); // start timer
            // Inserting parsed data into Mongodb
            collection.insertMany(record_rows, (err, res) => {
                time_insert = process.hrtime(start_insert); // end timer
                time_insert_s = ((time_insert[0] * NS_PER_SEC + time_insert[1])  * MS_PER_NS).toFixed(2);
                logger.info(`  Inserting table ${tableName} finished in ${time_insert_s} rows: ${nrows} (rows/s): ${(nrows/time_insert_s).toFixed()}`);
                resolve(true);
                return;
            });
        })
    })
}