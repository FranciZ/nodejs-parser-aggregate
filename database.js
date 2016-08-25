const mongoose = require('mongoose');

exports.connect = ()=>{

    return new Promise((resolve, reject)=>{

        mongoose.connect('mongodb://localhost/data-db');

        mongoose.connection.on('error', ()=>{
            reject();
        });

        mongoose.connection.once('open', ()=>{
            resolve();
        });

    });

};