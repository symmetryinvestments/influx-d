# influxdb-dlang-wrapper

[![Actions Status](https://github.com/symmetryinvestments/influx-d/workflows/CI/badge.svg)](https://github.com/symmetryinvestments/influx-d/actions)

D programming language wrapper for InfluxDB.

Generated documentation
-----------------------

[Integration Tests/Examples](http://influxdb.code.kaleidic.io/integration.html)

[API documentation](http://influxdb.code.kaleidic.io/influxdb.html)

Getting started tutorial (hopefully self-explanatory)
-----------------------------------------------------

```d

import influxdb;

// this will connect and create the `mydb` database if not already in InfluxDB
const database = Database("http://localhost:8086" /*URL*/, "mydb" /*DB name*/);

// no explicit timestamp
database.insert(Measurement("cpu" /*name*/,
                            ["tag1": "foo"] /*tags*/,
                            ["temperature": InfluxValue(42)] /*values*/));
// `insert` can also take `Measurement[]` or a variadic number of `Measurement`s
// Measurement also has a contructor that does't take tags:
// auto m = Measurement("cpu", ["temperature": InfluxValue(42)]);

// explicit timestamp
import std.datetime: Clock;
database.insert(Measurement("cpu",
                            ["tag1": "foo"],
                            ["temperature": InfluxValue(68)],
                            Clock.currTime));

// this will have the two measurements given the code above
const response = database.query("SELECT * FROM cpu");

// Accessing the response.
// The code below assumes a response with one result and that result has only
// one series.

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


About Kaleidic Associates
-------------------------
We are a boutique consultancy that advises a small number of hedge fund clients.  We are
not accepting new clients currently, but if you are interested in working either remotely
or locally in London or Hong Kong, and if you are a talented hacker with a moral compass
who aspires to excellence then feel free to drop me a line: laeeth at kaleidic.io

We work with our partner Symmetry Investments, and some background on the firm can be
found here:

http://symmetryinvestments.com/about-us/
