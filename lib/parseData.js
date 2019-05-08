
// Params
let config = require('../config/config');

/**
 * Parse each row in given table.
 *
 * @param {Object} row
 * @param {String} tableName
 * @return 
 */

module.exports = async function parseData(row, tableName) {
    // Filter null attributes
    if(config.delete_nulls) {
        for (var propName in row) { //foreach attribute
            if (row[propName] === null) { // if the property is null then delete
                delete row[propName];
            }
        }
    }

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
    // Parse extras
    row["extras"] = {};
    for (var propName in row) { // foreach attribute
        if (propName.search(/Posi_Extra_/i) !== -1) { // if attribute begins with "Posi_Extra"
            row.extras[propName] = row[propName];
            delete row[propName];
        }
    }
    return;
}