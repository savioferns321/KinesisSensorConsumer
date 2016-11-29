/*******************************************************************************
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License"). You may not use
 * this file except in compliance with the License. A copy of the License is
 * located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 ******************************************************************************/

'use strict';
var fs = require('fs');
var util = require('util');
var logger = require('../../util/logger');
var Converter = require("csvtojson").Converter;
var converter = new Converter({});
var sleep = require('sleep');

var filename = "../data/fsuResearchShipKAOUnrt_22d9_bc42_c17d.csv";

var hours, minutes, seconds, year, month, day, nyTrack = 0, sfTrack = 0, azTrack = 0, cityTrack = 0, trackLat = 0, trackLong = 0;

// to generate date to mysql format
(function() {

    Date.prototype.toYMD = Date_toYMD;

    function Date_toYMD(date) {

        hours = String(date.getHours());
        minutes = String(date.getMinutes());
        seconds = String(date.getSeconds());
        year = String(date.getFullYear());
        month = String(date.getMonth());
        day = String(date.getDate());

        var dt = year + "-" + month + "-" + day + " " + hours + "-"
            + minutes + "-" + seconds;

        return dt;
    }
})();

Date.prototype.toMysqlFormat = function(date) {
    return date.toYMD(date);
}

var cities = [ "New York", "San Fransisco", "Arizona City" ];
var cityLen = cities.length;

// New York Sensors
var NYlatitudes = [ 40.712784, 40.759011, 40.749599, 40.754931 ];
var NYlongitudes = [ -74.005941, -73.984472, -73.998936, -73.984019 ];
var nyLen = NYlatitudes.length;
var NYSensors = [["NY1", 1],["NY2",2],["NY3",3],["NY4",4]];


// San Fransisco Sensors
var SFlatitudes = [ 37.786453, 37.772066, 37.806053, 37.780649 ];
var SFlongitudes = [ -122.468719, -122.431153, -122.410331, -122.513758 ];
var SFSensors = [["SF1", 5],["SF2",6],["SF3",7],["SF4",8]];
var sfLen = SFlatitudes.length;

// Arizona City Sensors
var AZlatitudes = [ 32.901836, 32.879502, 32.755893, 32.755896 ];
var AZlongitudes = [ -111.742252, -111.757352, -111.670958, -111.554844 ];
var AZSensors = [["AZ1", 9],["AZ2",10],["AZ3",11],["AZ4",12]];
var azLen = AZlatitudes.length;

var anomalies=["fire","cyclone"];


function sampleProducer(kinesis, config) {
    var log = logger().getLogger('sampleProducer');

    function _createStreamIfNotCreated(callback) {
        var params = {
            ShardCount : config.shards,
            StreamName : config.stream
        };

        kinesis
            .createStream(
                params,
                function(err, data) {
                    if (err) {
                        if (err.code !== 'ResourceInUseException') {
                            callback(err);
                            return;
                        } else {
                            log
                                .info(util
                                    .format(
                                        '%s stream is already created. Re-using it.',
                                        config.stream));
                        }
                    } else {
                        log
                            .info(util
                                .format(
                                    "%s stream doesn't exist. Created a new stream with that name ..",
                                    config.stream));
                    }

                    // Poll to make sure stream is in ACTIVE state
                    // before start pushing data.
                    _waitForStreamToBecomeActive(callback);
                });
    }

    function _waitForStreamToBecomeActive(callback) {
        kinesis.describeStream({
            StreamName : config.stream
        }, function(err, data) {
            if (!err) {
                log.info(util.format('Current status of the stream is %s.',
                    data.StreamDescription.StreamStatus));
                if (data.StreamDescription.StreamStatus === 'ACTIVE') {
                    callback(null);
                } else {
                    setTimeout(function() {
                        _waitForStreamToBecomeActive(callback);
                    }, 1000 * config.waitBetweenDescribeCallsInSeconds);
                }
            }
        });
    }

    function _writeToKinesis(recordParams) {

        kinesis.putRecord(recordParams, function(err, data) {
            if (err) {
                console.log("Error"+err);
                log.error(err);
            } else {
                console.log("Success");
                log.info('Successfully sent data to Kinesis.');
            }
        });
    }

    return {
        run : function() {
            _createStreamIfNotCreated(function(err) {
                if (err) {
                    log.error(util.format('Error creating stream: %s', err));
                    return;
                }
                if (config.stream === "kclnodejssample") {
                    var count = 0;
                    fs.readFile(filename, function(err, data) {

                        if (err) {
                            throw err;
                        } else {
                            var dateT ;
                            // csv file will be parsed record by record
                            converter.on("record_parsed", function(jsonObj) {
                                cityTrack = (cityTrack % cityLen) + 1;

                                if (cities[cityTrack - 1] === "New York") {
                                    nyTrack = (nyTrack % nyLen) + 1;
                                    trackLat = NYlatitudes[0];
                                    trackLong = NYlongitudes[0];
                                    var date = new Date();
                                    dateT = new Date().toMysqlFormat(date)
                                    //sleep.sleep(1);
                                }
                                if (cities[cityTrack - 1] === "San Fransisco") {
                                    sfTrack = (sfTrack % sfLen) + 1;
                                    trackLat = SFlatitudes[0];
                                    trackLong = SFlongitudes[0];

                                }

                                if (cities[cityTrack - 1] == "Arizona City") {
                                    azTrack = (azTrack % azLen) + 1;
                                    trackLat = AZlatitudes[0];
                                    trackLong = AZlongitudes[0];
                                }


                                var aggregationParams = {
                                    "latitude" : trackLat,
                                    "longitude" : trackLong,
                                    "city" : cities[cityTrack - 1],
                                    "timestamp" : dateT,
                                    "temperature" : jsonObj.airTemperature,
                                    "CO2" : Math.round(Math.random() * (50 - 0)),
                                    "CO" : Math.round(Math.random() * (50 - 0)),
                                    "NO" : Math.round(Math.random() * (50 - 0)),
                                    "O3" : Math.round(Math.random() * (50 - 0)),
                                    "humidity" : jsonObj.relativeHumidity,
                                    "precipitation" : jsonObj.precipitationAmount,
                                    "windDirection" : (Math.random() * (360 - 0) + 0)
                                        .toFixed(2),
                                    "windSpeed" : jsonObj.windSpeed
                                };


                                var record = JSON.stringify(aggregationParams);
                                var recordParams = {
                                    Data : record,
                                    PartitionKey : aggregationParams.timestamp,
                                    StreamName : config.stream
                                };



                                setTimeout(function() {
                                    _writeToKinesis(recordParams)
                                }, 1000);

                            });
                            // read from file
                            fs.createReadStream(filename).pipe(converter);

                        }

                    })

                }else{

                    if(config.stream === "anomalyproducer"){

                        var tracktemp = 0, dateT, co =  0 , no=0 , co2 = 0, sensorTrack = {}, o3 = 0, anomalyTrack = 0, precipitation = 0, ws = 0, humidity = 0;

                        fs.readFile(filename, function(err, data) {

                            if (err) {
                                throw err;
                            } else {

                                converter.on("record_parsed", function(jsonObj) {


                                    anomalyTrack = anomalies[Math.floor(Math.random() * anomalies.length)];

                                    cityTrack = (cityTrack % cityLen) + 1;

                                    if (cities[cityTrack - 1] === "New York") {
                                        nyTrack = (nyTrack % nyLen) + 1;
                                        trackLat = NYlatitudes[nyTrack - 1];
                                        trackLong = NYlongitudes[nyTrack - 1];
                                        sensorTrack = NYSensors[nyTrack-1];
                                        var date = new Date();
                                        dateT = new Date().toMysqlFormat(date)
                                        //sleep.sleep(10);
                                    }
                                    if (cities[cityTrack - 1] === "San Fransisco") {
                                        sfTrack = (sfTrack % sfLen) + 1;
                                        trackLat = SFlatitudes[sfTrack - 1];
                                        trackLong = SFlongitudes[sfTrack - 1];
                                        sensorTrack = SFSensors[sfTrack - 1];
                                    }

                                    if (cities[cityTrack - 1] == "Arizona City") {
                                        azTrack = (azTrack % azLen) + 1;
                                        trackLat = AZlatitudes[azTrack - 1];
                                        trackLong = AZlongitudes[azTrack - 1];
                                        sensorTrack = AZSensors[azTrack - 1];
                                        anomalyTrack = "fire";
                                    }



                                    console.log(anomalyTrack);
                                    if(anomalyTrack === "fire"){

                                        tracktemp = (Math.random() * (1200 - 400) + 400)
                                            .toFixed(1);
                                        co = Math.round(Math.random() * (500 - 100)+100);
                                        co2 = Math.round(Math.random() * (500 - 100)+100);
                                        no = Math.round(Math.random() * (500 - 100)+100);
                                        o3 = Math.round(Math.random() * (500 - 100)+100);

                                    }else{
                                        tracktemp = jsonObj.airTemperature;
                                        co = Math.round(Math.random() * (50 - 0));
                                        co2 = Math.round(Math.random() * (50 - 0));
                                        no = Math.round(Math.random() * (50 - 0));
                                        o3 = Math.round(Math.random() * (50 - 0));
                                    }


                                    if(anomalyTrack === "cyclone"){

                                        humidity = 100;

                                        ws = (Math.random() * (300 - 69) + 69)
                                            .toFixed(1);

                                        precipitation = (Math.random() * (197 - 50) + 50)
                                            .toFixed(1)

                                    }else{
                                        humidity = jsonObj.relativeHumidity;
                                        precipitation  = jsonObj.precipitationAmount;
                                        ws = jsonObj.windSpeed
                                    }





                                    var date = new Date();
                                    var aggregationParams={
                                        "sensorName":sensorTrack[0],
                                        "sensorId":sensorTrack[1],
                                        "detectedAnomaly": anomalyTrack,
                                        "data":{
                                            "latitude" : trackLat,
                                            "longitude" : trackLong,
                                            "city" : cities[cityTrack - 1],
                                            "timestamp" : dateT,
                                            "temperature" : tracktemp,
                                            "CO2" : co2,
                                            "CO" : co,
                                            "NO" : no,
                                            "O3" : o3,
                                            "humidity" : humidity,
                                            "precipitation" : precipitation,
                                            "windDirection" : (Math.random() * (360 - 0) + 0)
                                                .toFixed(2),
                                            "windSpeed" : ws
                                        }
                                    };


                                    console.log(aggregationParams)

                                    var record = JSON.stringify(aggregationParams);
                                    var recordParams = {
                                        Data : record,
                                        PartitionKey : aggregationParams.data.timestamp,
                                        StreamName : config.stream
                                    };

                                    setTimeout(function() {
                                        _writeToKinesis(recordParams)
                                    }, 1000);



                                });

                                // read from file
                                fs.createReadStream(filename).pipe(converter);
                            }

                        })



                    }



                }
            });
        }
    };
}

module.exports = sampleProducer;