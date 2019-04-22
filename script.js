
var MsSqlClient   = require('mssql');
var MongoClient = require('mongodb').MongoClient;

var config = {};
// Params
config.sqlDatabase = 'Cybermapa_historico'; // Origin SQL database
config.mongoDatabase = 'gestya2'; // Destination Mongo collection
config.delete_nulls = true; // Do not insert null values into Mongo

config.mongodb = `mongodb://localhost:27017/${config.mongoDatabase}`;
config.mssql = `mssql://studio3t:studio3t@localhost:1434/${config.sqlDatabase}`;

const NS_PER_SEC = 1e9;
const MS_PER_NS = 1e-9;

function perTable(tableName, sql, mongo) {
    return new Promise((resolve, reject) => {
        let db = mongo.db(config.mongoDatabase);
        let collection = db.collection('posiciones')
    
        var request = new MsSqlClient.Request()
        let start = process.hrtime(); // start timer
        // Querying MSSQL
        request.query("SELECT * FROM " + tableName, (err, rows) => {
            if (err) console.log(err);
            let nrows = rows.recordsets[0].length; // get nrows of table
            time = process.hrtime(start); // end timer
            time_s = ((time[0] * NS_PER_SEC + time[1])  * MS_PER_NS).toFixed(2);
            console.log("  Querying table " + tableName + ' finished in ' + time_s + ' rows: ' + nrows + " ps (rows/s): " +  (nrows/time_s).toFixed());
            let record_rows = rows.recordsets[0]; // mssql rows
            // Filter null attributes
            if(config.delete_nulls) {
                for (let i = 0 ; i < record_rows.length ; i++) {
                    let row = record_rows[i];
                    for (var propName in row) { 
                        if (row[propName] === null) {
                          delete row[propName];
                        }
                    }
                }
                record_rows.forEach( row => {
                    Object.keys(row).forEach((key) => (row[key] == null) && delete row[key]);
                });
            }
            let start2 = process.hrtime(); // start timer
            // Inserting data into Mongodb
            collection.insertMany(record_rows, (err, res) => {
                time2 = process.hrtime(start2); // end timer
                time_s = ((time2[0] * NS_PER_SEC + time2[1])  * MS_PER_NS).toFixed(2);
                console.log("  Inserting table " + tableName + ' finished in ' + time_s);
                resolve(true);
            });
        })
    })
}

async function getTables(sql) {
    try {
        const result = await MsSqlClient.query(`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG='${config.sqlDatabase}'`);
        let tables = [];
        for (var i = 0; i < result.recordset.length; i++) {
            tables.push(result.recordset[i].TABLE_NAME);
        }
        return tables;
    } catch (err) {
        console.log('Error getting tables');
    }
}

(async function calling(){
    console.log('Connecting to DBs');
    try {
        var mongo = await MongoClient.connect(config.mongodb);
        var sql = await MsSqlClient.connect(config.mssql);
    } catch (err){
        console.log(err);
    }
    console.log('Getting tables');
    let start = process.hrtime();
    let tables = await getTables(sql);
    console.log('Getting tables finished in ' + process.hrtime(start));
    
    for(let i = 0 ; i < tables.length ; i++) {
        //await perTable('Posiciones_2013',sql, mongo);
        console.log("Querying table " + i + ' of ' + tables.length);
        //console.log(tables[i]);
        await perTable(tables[i],sql, mongo);
    }
    console.log('Finished!');
})()
