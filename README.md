# influxdb-dlang-wrapper

D programming language wrapper for InfluxDB.

Getting started tutorial (hopefully self-explantory):

```d

import influxdb;

// this will connect and create the `mydb` database if not already in InfluxDB
const database = Database("http://localhost:8086" /*URL*/, "mydb" /*DB name*/);

// no explicit timestamp
database.insert(Measurement("cpu", ["tag1": "foo"], ["temperature": 42]));
// insert can also take Measurement[] or a variadic number

// explicit timestamp
import std.datetime;
database.insert(Measurement("cpu", ["tag1": "foo"], ["temperature": 68], Clock.currTime));

// this will have the two measurements
const response = database.query("SELECT * FROM cpu");

// the code below assumes a response with one result and that result has only
// one series

assert(response.results.length == 1);
const result = response.results[0];
assert(result.statement_id == 0);
assert(result.series.length == 1);
const series = result.series[0];
assert(series.rows.length == 1);
const row = series.rows[0];

assert(row.time == SysTime(DateTime(2015, 06, 11, 20, 46, 2), UTC()));
assert(row["foo"] == "bar");

assert(series ==
        MeasurementSeries(
            "lename", //name
            ["time", "othervalue", "tag1", "tag2", "value"], //column names
            //values
            [
                ["2015-06-11T20:46:02Z", "4", "toto", "titi", "2"],
                ["2017-03-14T23:15:01.06282785Z", "3", "letag", "othertag", "1"],
            ]
        ));
```
