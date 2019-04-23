
var MsSqlClient   = require('mssql');
var MongoClient = require('mongodb').MongoClient;

var config = {};
// Params
config.sqlDatabase = 'Cybermapa_historico'; // Origin SQL database
config.mongoDatabase = 'gestya3'; // Destination Mongo collection
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
        let start_select = process.hrtime(); // start timer
        // Querying MSSQL
        request.query("SELECT * FROM " + tableName, async (err, rows) => {
            if (err) console.log(err);
            let nrows = rows.recordsets[0].length; // get nrows of table
            time_select = process.hrtime(start_select); // end timer
            time_select_s = ((time_select[0] * NS_PER_SEC + time_select[1])  * MS_PER_NS).toFixed(2);
            console.log("  Querying table " + tableName + ' finished in ' + time_select_s + ' rows: ' + nrows + " ps (rows/s): " +  (nrows/time_select_s).toFixed());
            let record_rows = rows.recordsets[0]; // mssql rows
            // Loop for rows in a given table
            for (let i = 0 ; i < record_rows.length ; i++) {
                let row = record_rows[i];
                // Parse Data
                await parseData(tableName, row);
            }
            let start_insert = process.hrtime(); // start timer
            // Inserting data into Mongodb
            collection.insertMany(record_rows, (err, res) => {
                time_insert = process.hrtime(start_insert); // end timer
                time_insert_s = ((time_insert[0] * NS_PER_SEC + time_insert[1])  * MS_PER_NS).toFixed(2);
                console.log("  Inserting table " + tableName + ' finished in ' + time_insert_s);
                resolve(true);
            });
        })
    })
}

async function parseData(tableName, row) {
    // if(!row.Posi_longitud) {
    //     console.log(row);
    //     return
    // }
    // Insert Vehicle number attribute
    row['vehiculo_id'] = parseInt(tableName.slice(11));
    // Parse coordinates to GEOjson
    row["location"] = {
        type: "Point",
        coordinates: [row.Posi_longitud, row.Posi_latitud] //the longitude first and then latitude
    }
    delete row.Posi_longitud; // delete old values
    delete row.Posi_latitud;
    // Parse satelites
    row["satelites"] = {
        posi_satelites_usados: row.posi_satelites_usados,
        posi_satelites_vistos: row.posi_satelites_vistos,
        posi_satelites_supuestos: row.posi_satelites_supuestos,
    }
    delete row.posi_satelites_usados; // delete old values
    delete row.posi_satelites_vistos;
    delete row.posi_satelites_supuestos;
    // Parse sensores
    row["sensores"] = {
        posi_sensor1: row.posi_sensor1,
        posi_sensor2: row.posi_sensor2,
        posi_sensor3: row.posi_sensor3,
    }
    delete row.posi_sensor1; // delete old values
    delete row.posi_sensor2;
    delete row.posi_sensor3;
    // Filter null attributes
    if(config.delete_nulls) {
        for (var propName in row) { //foreach attribute
            if (row[propName] === null) { // if the property is null then delete
                delete row[propName];
            }
        }
    }
    // Parse extras
    row["extras"] = {};
    for (var propName in row) { //foreach attribute
        if (propName.search(/Posi_Extra_/i) !== -1) { 
            row.extras[propName] = row[propName];
            delete row[propName];
        }
    }
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
