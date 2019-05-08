    
// Configure logging
let winston = require('winston');
const { combine, timestamp, label, prettyPrint, printf  } = winston.format;

const myFormat = printf(({ level, message, label, timestamp }) => {
    return `${timestamp} ${level}: ${message}`;
});

module.exports = winston.createLogger({
    format: 
        combine( 
            timestamp(),
            prettyPrint(),
            myFormat
        ),
    transports: [
        new winston.transports.Console({ format: winston.format.simple() }),
        new winston.transports.File({ filename: './logs/combined.log' })
    ]
})

