'use strict';

const fs            = require('fs');
const _             = require('lodash');
const csv           = require('fast-csv');
const database      = require('./database');
const mongoose      = require('mongoose');
const lineReader    = require('line-reader');
const argv          = require('yargs').argv;

require('./resources/transaction/transaction-model');
require('./resources/davcni_zavezanec/dz-model');
require('./resources/pu/pu-model');
require('./resources/skd/skd-model');
require('./resources/skd_2002/skd2002-model');

const constants = require('./constants');

var stream = fs.createReadStream("docs/trans2003.csv");

database.connect()
    .then(()=>{
        
        if(argv.trans){
            if(!argv.path) return new Error('Missing path');
            parseCsv(argv.path);
        }else if(argv.po){
            if(!argv.path) return new Error('Missing path');
            parseLinesPO(argv.path);
        }else if(argv.dej){
            if(!argv.path) return new Error('Missing path');
            parseLinesDej(argv.path);
        }else if(argv.fo){
            if(!argv.path) return new Error('Missing path');
            parseLinesFo(argv.path);
        }else if(argv.pu){
            if(!argv.path) return new Error('Missing path');
            parsePu(argv.path);
        }else if(argv.skd){
            if(!argv.path) return new Error('Missing path');
            parseSKD(argv.path);
        }else if(argv.skd2002){
            if(!argv.path) return new Error('Missing path');
            parseSKD2002(argv.path);
        }else if(argv.ddv){
            groupByDDV();
        }

    });

function groupByDDV(){

    const TransactionModel = mongoose.model('Transaction');

    TransactionModel.aggregate(
        {
            $match:{ davcna_stevilka:'80267432' }
        },
        {
            $group:{
                _id:'$sifra_pu',
                total:{ $sum:'$znesek_transakcije' }
            }
        },
        {
            $sort:{
                total:1
            }
        },
        {
            $lookup:{
                from:'pus',
                localField:'_id',
                foreignField:'sifra_pu',
                as:'pu'
            }
        },
        {
            $unwind:'$pu'
        }

    ).exec((err, docs)=>{

        if(!err){
            console.log(docs);
        }else{
            console.log(err);
        }

    });

}

function parseSKD2002(path){

    const SKD2002 = mongoose.model('SKD2002');

    console.log('Parse skd2002');
    var stream = fs.createReadStream(path);
    let count = 0;
    var csvStream = csv({
        headers:true,
        delimiter:';'
    })
        .transform((data, next)=>{

            var skd = new SKD2002(data);

            skd.save()
                .then(()=>{
                    count++;
                    console.log('Saved: ',count);
                    next();
                });

        })
        .on("data", function(data){

        })
        .on("end", function(){
            console.log("done");
        });

    stream.pipe(csvStream);

}

function parseSKD(path){

    let count = 0;
    var stream = fs.createReadStream(path);
    const SKDModel = mongoose.model('SKD');

    var csvStream = csv({
        headers:true,
        delimiter:';'
    })
        .transform((data, next)=>{
            data.sifra_kategorije = data.sifra_kategorije.replace(/ /, '');
            data.sifra_starsa = data.sifra_starsa.replace(/ /, '');

            var skd = new SKDModel(data);

            skd.save()
                .then(()=>{
                    count++;
                    console.log('Saved: ',count);
                    next();
                });

        })
        .on("data", function(data){

        })
        .on("end", function(){
            console.log("done");
        });

    stream.pipe(csvStream);

}

function parsePu(path){

    let count = 0;
    var stream = fs.createReadStream(path);
    const PuModel = mongoose.model('Pu');

    var csvStream = csv({
        headers:true,
        delimiter:';'
    })
        .transform((data, next)=>{
            var pu = new PuModel(data);

            pu.save()
                .then(()=>{
                    count++;
                    console.log('Saved: ',count);
                    next();
                });
        })
        .on("data", function(data){

        })
        .on("end", function(){
            console.log("done");
        });

    stream.pipe(csvStream);

}

function parseLinesDej(path){

    console.log('parse: ',path);

    let lineCount = 0;
    const DavcniZavezanec = mongoose.model('DavcniZavezanecModel');

    lineReader.eachLine(path, function(line, last, cb) {

        if (last) {
            cb(false); // stop reading
        } else {

            line = line.replace(/\t/g, '    ');

            var col1 = line.substr(0,8-0);
            var col2 = line.substr(9,19-9);
            var col3 = line.substr(20,27-20);
            var col4 = line.substr(27,128-27);
            var col5 = line.substr(128,243-128);
            var col6 = line.substr(242,244-242);

            var data = {

                ddv:col1,
                maticna:col2,
                sifra_dejavnosti:col3,
                ime:col4,
                naslov:col5,
                financni_urad:col6,
                type:constants.DZ.TYPE.DEJ
            };

            const dz = new DavcniZavezanec(data);

            dz.save()
                .then(()=>{
                    lineCount++;
                    console.log('Saved: ',lineCount);
                    cb();
                })
                .catch((err)=>{
                    console.log(err);
                });
        }
    });

}

function parseLinesFo(path){

    console.log('parse: ',path);

    let lineCount = 0;
    const DavcniZavezanec = mongoose.model('DavcniZavezanecModel');

    lineReader.eachLine(path, function(line, last, cb) {

        if (last) {
            cb(false); // stop reading
        } else {

            line = line.replace(/\t/g, '    ');

            var col1 = line.substr(0,1);
            var col2 = line.substr(2,10-2);
            var col3 = line.substr(11,72-11);
            var col4 = line.substr(72,184-72);
            var col5 = line.substr(184,194-184);
            var col6 = line.substr(195,197-195);

            if(col1 === 'P'){
                col1 = true;
            }else{
                col1 = false;
            }

            var date = new Date();
            var dateParts = col5.split('.');
            var day = parseInt(dateParts[0]);
            var month = parseInt(dateParts[1]);
            var year = parseInt(dateParts[2]);
            date.setDate(day);
            date.setMonth(month-1);
            date.setYear(year);

            var data = {
                ni_placnik:col1,
                ddv:col2,
                ime:col3,
                naslov:col4,
                datum_registracije:date,
                financni_urad:col6,
                type:constants.DZ.TYPE.FO
            };

            const dz = new DavcniZavezanec(data);

            dz.save()
                .then(()=>{
                    lineCount++;
                    console.log('Saved: ',lineCount);
                    cb();
                })
                .catch((err)=>{
                    console.log(err);
                });
        }
    });

}

function parseLinesPO(path){

    console.log('parse: ',path);

    let lineCount = 0;
    const DavcniZavezanec = mongoose.model('DavcniZavezanecModel');

    lineReader.eachLine(path, function(line, last, cb) {

        if (last) {
            cb(false); // stop reading
        } else {

            line = line.replace(/\t/g, '    ');

            var col1 = line.substr(1,2-1);
            var col2 = line.substr(2,3-2);
            var col3 = line.substr(4,12-4);
            var col4 = line.substr(13,23-13);
            var col5 = line.substr(24,34-24);
            var col6 = line.substr(35,41-35);
            var col7 = line.substr(42,143-42);
            var col8 = line.substr(143,257-143);
            var col9 = line.substr(257,259-257);

            if(col1 === 'P'){
                col1 = true;
            }else{
                col1 = false;
            }

            if(col2 === '*'){
                col2 = true;
            }else{
                col2 = false;
            }

            var date = new Date();
            var dateParts = col5.split('.');
            var day = parseInt(dateParts[0]);
            var month = parseInt(dateParts[1]);
            var year = parseInt(dateParts[2]);
            date.setDate(day);
            date.setMonth(month-1);
            date.setYear(year);

            var data = {
                ni_placnik:col1,
                zavezanec:col2,
                ddv:col3,
                maticna:col4,
                datum_registracije:date,
                sifra_dejavnosti:col6,
                ime:col7,
                naslov:col8,
                financni_urad:col9,
                type:constants.DZ.TYPE.PO
            };

            const dz = new DavcniZavezanec(data);

            dz.save()
                .then(()=>{
                    lineCount++;
                    console.log('Saved: ',lineCount);
                   cb();
                })
                .catch((err)=>{
                    console.log(err);
                });
        }
    });

}

function parseCsv(path){

    const TransactionModel = mongoose.model('Transaction');

    var csvStream = csv({
        headers:true,
        delimiter:';'
    })
        .transform((data, next)=>{

            var transaction = new TransactionModel(data);

            transaction.save()
                .then(()=>{
                    next();
                });

        })
        .on("data", function(data){

        })
        .on("end", function(){
            console.log("done");
        });

    stream.pipe(csvStream);

}