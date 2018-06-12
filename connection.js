var elasticsearch = require('elasticsearch');

let username = 'elastic';
let password = '******';
let server = '******';
let port = '9243';

var client = new elasticsearch.Client({
    hosts: [
        'https://' + username + ':' + password + '@' + server + ':' + port + '/'
    ]
});

module.exports = client;
