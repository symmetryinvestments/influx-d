# influxdb-dlang-wrapper

D programming language wrapper for InfluxDB.

Getting started tutorial (hopefully self-explantory):

```d

import influxdb;

// this will connect and create the `mydb` database if not already in InfluxDB
auto database = Database("http://localhost:8086" /*URL*/, "mydb" /*DB name*/);

// no explicit timestamp
database.insert(Measurement("cpu", ["tag1": "foo"], ["temperature": 42]));
// insert can also take Measurement[] or a variadic number

// explicit timestamp
import std.datetime;
database.insert(Measurement("cpu", ["tag1": "foo"], ["temperature": 68], Clock.currTime));

// this will have the two measurements
auto json = database.query("SELECT * FROM cpu");

```
