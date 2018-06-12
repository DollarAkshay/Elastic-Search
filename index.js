const client = require('./connection.js');

let lineOffset = 1;
let lineReader = require('line-reader');

let total = 16783023;
let batchSize = 40000;
let allLines = [];
let bodyData = [];

function readFile () {
    lineReader.eachLine('Post Activity - Share Only.csv', function (line, last) {
        allLines.push(line);
        if (last) {
            console.log('Done Reading File');
            uploadToElasticSearch();
        }
    });
}

function fetchLines () {
    var lines = allLines.splice(1, batchSize);

    lines.forEach(function (line, i) {
        var words = line.split(',').map(word => word.replace(/"/g, ''));
        bodyData.push({
            'index': {
                '_index': 'post_activity',
                '_type': '_doc',
                '_id': lineOffset + i
            }
        });

        bodyData.push({
            'user_id': parseInt(words[0]),
            'activity': words[1],
            'language': words[2],
            'time_stamp': parseInt(words[3])
        });
    });

    lines = null;
    console.log('Fetched Lines from ' + lineOffset + ' to ' + (lineOffset + batchSize - 1));
}

// Function uploads data to elastic search
function uploadToElasticSearch () {
    fetchLines(lineOffset, lineOffset + batchSize);
    lineOffset += batchSize;

    client.bulk({body: bodyData}, function (err, resp) {
        if (err) {
            console.log('Error Occured !');
            console.log(err);
        }
        bodyData = [];
        console.log('Time Taken :', resp.took / 1000, 'sec');
        console.log((100 * Math.min(1, lineOffset / total)).toFixed(2) + '% done');
        uploadToElasticSearch();
    });
}

readFile();
