var express = require("express");
var moment = require("moment");
var http = require('http');
var request = require('request');
var fs = require('fs');
var Q = require('q');
var cors = require('cors');
var foundCurrentData = false;
var app = express();
var port = process.env.PORT || 7000;
var currentHour = moment().hour();
var baseDir = 'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_1p00.pl?dir=%2Fgfs.' + moment().format('YYYYMMDD') + '%2F' + getHourSegment(currentHour);
//var baseDir = 'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?dir=%2Fgfs.' + moment().format('YYYYMMDD') + '%2F' + getHourSegment(currentHour);
function getHourSegment(hour) {
    if (hour >= 0 && hour < 6) {
        return '00';
    } else if (hour >= 6 && hour < 12) {
        return '06';
    } else if (hour >= 12 && hour < 18) {
        return '12';
    } else {
        return '18';
    }
}


// cors config
var whitelist = [
    'http://localhost:63342',
    'http://localhost:3000',
    'http://localhost:4000',
    'http://danwild.github.io'
];

var corsOptions = {
    origin: function(origin, callback){
        var originIsWhitelisted = whitelist.indexOf(origin) !== -1;
        callback(null, originIsWhitelisted);
    }
};

app.listen(port, function(err){
    console.log("running server on port "+ port);
});

app.get('/', cors(corsOptions), function(req, res){
    res.send('hello wind-js-server.. go to /latest for wind data..');
});

app.get('/alive', cors(corsOptions), function(req, res){
    res.send('wind-js-server is alive');
});

app.get('/latest', cors(corsOptions), function(req, res){

    /**
     * Find and return the latest available 6 hourly pre-parsed JSON data
     *
     * @param targetMoment {Object} UTC moment
     */
    function sendLatest(targetMoment){

        var stamp = moment(targetMoment).format('YYYYMMDD') + roundHours(moment(targetMoment).hour(), 6);
        var fileName = __dirname +"/json-data/"+ stamp +".json";

        res.setHeader('Content-Type', 'application/json');
        res.sendFile(fileName, {}, function (err) {
            if (err) {
                console.log(stamp +' doesnt exist yet, trying previous interval..');
                sendLatest(moment(targetMoment).subtract(6, 'hours'));
            }
        });
    }

    sendLatest(moment().utc());

});

app.get('/nearest', cors(corsOptions), function(req, res, next){

    var time = req.query.timeIso;
    var limit = req.query.searchLimit;
    var searchForwards = false;

    /**
     * Find and return the nearest available 6 hourly pre-parsed JSON data
     * If limit provided, searches backwards to limit, then forwards to limit before failing.
     *
     * @param targetMoment {Object} UTC moment
     */
    function sendNearestTo(targetMoment){

        if( limit && Math.abs( moment.utc(time).diff(targetMoment, 'days'))  >= limit) {
            if(!searchForwards){
                searchForwards = true;
                sendNearestTo(moment(targetMoment).add(limit, 'days'));
                return;
            }
            else {
                return next(new Error('No data within searchLimit'));
            }
        }

        var stamp = moment(targetMoment).format('YYYYMMDD') + roundHours(moment(targetMoment).hour(), 6);
        var fileName = __dirname +"/json-data/"+ stamp +".json";

        res.setHeader('Content-Type', 'application/json');
        res.sendFile(fileName, {}, function (err) {
            if(err) {
                var nextTarget = searchForwards ? moment(targetMoment).add(6, 'hours') : moment(targetMoment).subtract(6, 'hours');
                sendNearestTo(nextTarget);
            }
        });
    }

    if(time && moment(time).isValid()){
        sendNearestTo(moment.utc(time));
    }
    else {
        return next(new Error('Invalid params, expecting: timeIso=ISO_TIME_STRING'));
    }

});




/**
 * Ping for new data every 6 hours
 */
function run(targetMoment){
    console.log("Start of run function.");
    var exec = require('child_process').exec, child;
    exec('rm grib-data/*');
    exec('rm json-data/*');
    // Fetch data
    getGribData(targetMoment).then(function(response){
        if(response.stamp){
            convertGribToJson(response.stamp, response.targetMoment);
        }
    });
    
    // Calculate next run time
    const nextRunTime = moment.utc().add(6, 'hours');
    let timeUntilNextRun = nextRunTime.diff(moment.utc());

    console.log("Scheduled next run to start in approximately", timeUntilNextRun, "milliseconds.");

    // Log the countdown timer
    logCountdown(timeUntilNextRun);

    // Schedule next run after 5 minutes
    const timeoutId = setTimeout(function() {
        console.log("Set timeout triggered.");
        clearTimeout(timeoutId); // Clear the previous timeout
        run(moment.utc());
    }, timeUntilNextRun);
}

// Function to log the countdown timer
function logCountdown(time) {
    const interval = 3600000; // Update the countdown every hour (3600 seconds * 1000 milliseconds)
    let remainingTime = time;

    const countdownInterval = setInterval(function() {
        console.log("Countdown:", remainingTime / 3600000, "hours remaining until the next run.");

        remainingTime -= interval;

        if (remainingTime <= 0) {
            clearInterval(countdownInterval);
        }
    }, interval);
}

/**
 *
 * Finds and returns the latest 6 hourly GRIB2 data from NOAAA
 *
 * @returns {*|promise}
 */
function getGribData(targetMoment){

    var deferred = Q.defer();

    function runQuery(targetMoment){

        if (!moment(targetMoment).isSame(moment(), 'day')) {
            console.log('Reached end of today\'s data.');
            return;
        }

        var stamp = moment(targetMoment).format('YYYYMMDD') + roundHours(moment(targetMoment).hour(), 6);
        var urlstamp = stamp.slice(0,8)+'/'+stamp.slice(8,10)+'/atmos';
        request.get({
            url: baseDir,
            qs: {
                file: 'gfs.t'+ roundHours(moment(targetMoment).hour(), 6) +'z.pgrb2.1p00.f000',
                lev_10_m_above_ground: 'on',
                lev_surface: 'on',
                var_UGRD: 'on',
                var_VGRD: 'on',
                leftlon: 0, //-360
                rightlon: 360, 
                toplat: 90,
                bottomlat: -90,
                dir: '/gfs.'+urlstamp
            }

        }).on('error', function(err){
            // console.log(err);
            runQuery(moment(targetMoment).subtract(6, 'hours'));

        }).on('response', function(response) {

            console.log('response '+response.statusCode + ' | '+stamp+'.f000');

            if(response.statusCode != 200){
                runQuery(moment(targetMoment).subtract(6, 'hours'));
            }

            else {
                foundCurrentData = true
                // don't rewrite stamps
                if(!checkPath('/var/www/html/weather/tile/wind_particles/'+ stamp +'.f000.json', false)) {

                    console.log('piping ' + stamp);

                    // mk sure we've got somewhere to put output
                    checkPath('grib-data', true);

                    // pipe the file, resolve the valid time stamp
                    var file = fs.createWriteStream("grib-data/"+stamp+".f000");
                    response.pipe(file);
                    file.on('finish', function() {
                        file.close();
                        deferred.resolve({stamp: stamp, targetMoment: targetMoment});
                    });

                }
                else {
                    console.log('already have '+ stamp +', not looking further');
                    deferred.resolve({stamp: false, targetMoment: false});
                }
            }
        });

    }

    runQuery(targetMoment);
    return deferred.promise;
}

function convertGribToJson(stamp, targetMoment){

    // mk sure we've got somewhere to put output
    checkPath('json-data', true);

    var exec = require('child_process').exec, child;

    child = exec('converter/bin/grib2json --data --output /var/www/html/weather/tile/wind_particles/'+stamp+'.json --names --compact grib-data/'+stamp+'.f000',
        {maxBuffer: 500*1024},
        function (error, stdout, stderr){

            if(error){
                console.log('exec error: ' + error);
            }

            else {
                console.log("converted..");

                // don't keep raw grib data
                // exec('rm grib-data/*');

                // if we don't have older stamp, try and harvest one
                var prevMoment = moment(targetMoment).subtract(6, 'hours');
                var prevStamp = prevMoment.format('YYYYMMDD') + roundHours(prevMoment.hour(), 6);

                if(!checkPath('/var/www/html/weather/tile/wind_particles/'+ prevStamp +'.json', false)){

                    console.log("Run forecast for: "+ stamp);
                    runForecast(targetMoment);
                }

                else {
                    console.log('got older, no need to harvest further');
                }
            }
        });
}

function getGribDataForecast(targetMoment, forecast, hours){

    var deferred = Q.defer();

    function runQueryForecast(targetMoment){

        if (!moment(targetMoment).isSame(moment(), 'day')) {
            console.log('Reached end of today\'s data.');
            return;
        }

        var stamp = moment(targetMoment).format('YYYYMMDD') + roundHours(moment(targetMoment).hour(), 6);
        var urlstamp = stamp.slice(0,8)+'/'+stamp.slice(8,10)+'/atmos';
        request.get({
            url: baseDir,
            qs: {
                file: 'gfs.t'+ roundHours(moment(targetMoment).hour(), 6) +'z.pgrb2.1p00.' + forecast,
                lev_10_m_above_ground: 'on',
                lev_surface: 'on',
                var_UGRD: 'on',
                var_VGRD: 'on',
                leftlon: 0, //-360
                rightlon: 360,
                toplat: 90,
                bottomlat: -90,
                dir: '/gfs.'+urlstamp
            }

        }).on('error', function(err){
            console.log(err);

        }).on('response', function(response) {

            console.log('response '+response.statusCode + ' | '+stamp+'.'+forecast);

            if(response.statusCode != 200){
                 console.log('response '+response.statusCode + ' | '+stamp);
            }

            else {

                // don't rewrite stamps
                if(!checkPath('/var/www/html/weather/tile/wind_particles/'+ moment(stamp, "YYYYMMDDHH").add(hours, 'hours').format("YYYYMMDDHH") +'.json', false)) {

                    console.log('piping ' + stamp +'.'+forecast);

                    // mk sure we've got somewhere to put output
                    checkPath('grib-data', true);

                    // pipe the file, resolve the valid time stamp
                    var file = fs.createWriteStream("grib-data/"+stamp+"."+forecast);
                    response.pipe(file);
                    file.on('finish', function() {
                        file.close();
                        deferred.resolve({stamp: stamp, targetMoment: targetMoment, forecast: forecast, hours: hours});
                    });

                }
                else {
                    console.log('already have '+ stamp +', not looking further');
                    deferred.resolve({stamp: false, targetMoment: false});
                }
            }
        });

    }

    runQueryForecast(targetMoment);
    return deferred.promise;
}

function convertGribToJsonForecast(stamp, targetMoment, forecast, hours){

    const outputFile = `/var/www/html/weather/tile/wind_particles/${moment(stamp, "YYYYMMDDHH").add(hours, 'hours').format("YYYYMMDDHH")}.json`;
    const inputFile = `grib-data/${stamp}.${forecast}`;

    const command = `converter/bin/grib2json --data --compact ${inputFile}`;

    exec(command, { maxBuffer: 500 * 1024 }, (error, stdout, stderr) => {
        if (error) {
            console.log('exec error: ' + error);
        } else {
            console.log(`${inputFile} converted to JSON..`);

            try {
                // Parse stdout as JSON
                const jsonData = JSON.parse(stdout);

                // Recursively round numerical values
                const roundedData = roundNumbers(jsonData);

                // Write rounded JSON data to output file
                fs.writeFile(outputFile, JSON.stringify(roundedData, null, 2), (err) => {
                    if (err) {
                        console.error('Error writing rounded JSON:', err);
                    } else {
                        console.log(`Rounded JSON data written to ${outputFile}`);
                    }
                });
            } catch (err) {
                console.error('Error parsing or rounding JSON:', err);
            }
        }
    });
}

function roundNumbers(obj) {
    if (typeof obj === 'number') {
        return parseFloat(obj.toFixed(2));
    } else if (Array.isArray(obj)) {
        return obj.map(roundNumbers);
    } else if (typeof obj === 'object' && obj !== null) {
        const newObj = {};
        for (const key in obj) {
            newObj[key] = roundNumbers(obj[key]);
        }
        return newObj;
    }
    return obj;
}

function runForecast(targetMoment){
    for (var i = 6; i <= 120; i += 6) {
        var forecast = 'f' + i.toString().padStart(3, '0');
        getGribDataForecast(targetMoment, forecast, i).then(function(response){

            if(response.stamp){
                convertGribToJsonForecast(response.stamp, response.targetMoment, response.forecast, response.hours);
            }
        });
    }
}
/**
 *
 * Round hours to expected interval, e.g. we're currently using 6 hourly interval
 * i.e. 00 || 06 || 12 || 18
 *
 * @param hours
 * @param interval
 * @returns {String}
 */
function roundHours(hours, interval){
    if(interval > 0){
        var result = (Math.floor(hours / interval) * interval);
        return result < 10 ? '0' + result.toString() : result;
    }
}

/**
 * Sync check if path or file exists
 *
 * @param path {string}
 * @param mkdir {boolean} create dir if doesn't exist
 * @returns {boolean}
 */
function checkPath(path, mkdir) {
    try {
        fs.statSync(path);
        return true;

    } catch(e) {
        if(mkdir){
            fs.mkdirSync(path);
        }
        return false;
    }
}

// init harvest
run(moment.utc());