/**
    This module implements integration tests for Influx API

    Authors: Atila Neves (Kaleidic Associates Advisory Limited)

    Generated documentation:
        http://influxdb.code.kaleidic.io/influxdb.html

*/
module integration.api;

import unit_threaded;
import influxdb;
import integration.common: influxURL;


///
@Serial
@("Database api")
unittest {

    import influxdb.api: Database, Measurement;

    const database = Database(influxURL, "myspecialDB");
    scope(exit) database.drop;

    database.insert(Measurement("cpu", ["tag1": "foo"], ["temperature": "42"]));
    database.insert(Measurement("cpu", ["tag1": "foo"], ["temperature": "68"]));

    {
        const response = database.query("SELECT * from cpu");
        const result = response.results[0];
        const series = result.series[0];
        series.rows.length.shouldEqual(2);
    }

    {
        const response = database.query("SELECT * from cpu WHERE temperature > 50");
        const result = response.results[0];
        const series = result.series[0];
        series.rows.length.shouldEqual(1);
    }
}

///
@Serial
@("Database multiple inserts")
unittest {

    import influxdb.api: Database, Measurement;

    const database = Database(influxURL, "myspecialDB");
    scope(exit) database.drop;

    database.insert(Measurement("cpu", ["tag1": "foo"], ["temperature": "42"]),
                    Measurement("cpu", ["tag1": "bar"], ["temperature": "68"]),
                    Measurement("cpu", ["tag1": "baz"], ["temperature": "54"]));

    const response = database.query("SELECT * from cpu WHERE temperature > 50");
    const result = response.results[0];
    const series = result.series[0];
    series.rows.length.shouldEqual(2);
}

///
@Serial
@("Database explicit timestamps")
unittest {

    import influxdb.api: Database, Measurement;
    import std.datetime;

    const database = Database(influxURL, "myspecialDB");
    scope(exit) database.drop;

    database.insert(Measurement("cpu", ["tag1": "foo"], ["temperature": "42"], SysTime(DateTime(2017, 1, 1))));
    database.insert(Measurement("cpu", ["tag1": "foo"], ["temperature": "68"], SysTime(DateTime(2015, 1, 1))));

    {
        const response = database.query("SELECT * from cpu");
        const result = response.results[0];
        const series = result.series[0];
        series.rows.length.shouldEqual(2);
    }

    {
        const response = database.query("SELECT * from cpu WHERE time >= '2016-01-01 00:00:00'");
        const result = response.results[0];
        const series = result.series[0];
        series.rows.length.shouldEqual(1);
    }

}

@Serial
@("string data")
unittest {
    import influxdb.api: Database, Measurement;

    const database = Database(influxURL, "myspecialDB");
    scope(exit) database.drop;

    database.insert(Measurement("cpu", ["string": "foo"]));
}

@Serial
@("float data")
unittest {
    import influxdb.api: Database, Measurement;

    const database = Database(influxURL, "myspecialDB");
    scope(exit) database.drop;

    database.insert(Measurement("cpu", ["value": "42.3"]));
}

@Serial
@("bool data")
unittest {
    import influxdb.api: Database, Measurement;

    const database = Database(influxURL, "myspecialDB");
    scope(exit) database.drop;

    database.insert(Measurement("cpu", ["value": "true"]));
    database.insert(Measurement("cpu", ["value": "false"]));
}
