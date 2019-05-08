
// Code params
module.exports = {
    delete_nulls: true, // do not insert null values into Mongo
    nWorkers: Math.min(require('os').cpus().length, 2) // number of child processes
};