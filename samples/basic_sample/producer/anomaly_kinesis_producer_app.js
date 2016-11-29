/**
 * http://usejsdoc.org/
 */

'use strict';

var AWS = require('aws-sdk');
var config = require('./config');
var producer = require('./sample_producer');

var kinesis = new AWS.Kinesis({region : config.kinesis.region});
producer(kinesis, config.anomalyProducer).run();