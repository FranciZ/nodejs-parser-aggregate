const express = require('express');
const server = express();
const cors = require('cors');

server.use(cors());

exports.start = ()=>{

    return new Promise((resolve, reject)=>{

        server.listen(3030, function(){
            resolve();
        });

    });

};

exports.getApp = ()=>{

    return server;

};