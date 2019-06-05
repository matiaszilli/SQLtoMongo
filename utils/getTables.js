
/**
 * Get a list of all tables in a given database.
 *
 * @param {Object} MsSqlConnection
 * @param {String} database
 * @return {Array}
 */

module.exports = async function getTables(MsSqlConnection, database) {
    try {
        const result = await MsSqlConnection.query(`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG='${database}'`);
        let tables = [];
        let tableName;
        for (var i = 0; i < result.recordset.length; i++) {
            tableName = result.recordset[i].TABLE_NAME;
            if(tableName.indexOf("Posiciones_") === 0) // just elements which begins with "Posiciones_"
                tables.push(tableName);
        }
        return tables;
    } catch (err) {
        console.log('Error getting tables');
    }
}