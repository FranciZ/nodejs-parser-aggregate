const database = require('./database');
const server = require('./server');

function init(){

    database.connect()
        .then(server.start)
        .then(()=>{
            require('./resources')(server.getApp());
        })
        .then(()=>{
            console.log('All is well');
        })
        .catch((err)=>{
            console.log(err);
        });

}

init();