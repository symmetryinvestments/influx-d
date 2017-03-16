/**
 This module makes sure that using vibe corresponds in using the HTTP API
 correctly.
 */
module integration.vibe;

import unit_threaded;
import integration.common: influxURL;
import influxdb.vibe: manage, query, write;
import std.json: JSONValue, parseJSON;


@Serial
@("manage")
unittest {
    manage(influxURL, "DROP DATABASE test_vibe_db");
    wait;
    manage(influxURL, "CREATE DATABASE test_vibe_db");
    wait;
    manage(influxURL, "DROP DATABASE test_vibe_db");
    wait;
}


@Serial
@("query empty database")
unittest {
    manage(influxURL, "DROP DATABASE test_vibe_db");
    wait;
    manage(influxURL, "CREATE DATABASE test_vibe_db");
    wait;
    scope(exit) {
        manage(influxURL, "DROP DATABASE test_vibe_db");
        wait;
    }

    const json = query(influxURL, "test_vibe_db", "SELECT * from foo").parseJSON;
    JSONValue expected;
    JSONValue result;
    result["statement_id"] = JSONValue(0);
    expected["results"] = [result];
    json.shouldEqual(expected);
}

/*
  Example of a response (prettified):
  {
    "results": [{
            "series": [{
                    "columns": ["time", "othervalue", "tag1", "tag2", "value"],
                    "name": "foo",
                    "values": [
                            ["2015-06-11T20:46:02Z", 4, "toto", "titi", 2],
                            ["2017-03-14T23:15:01.06282785Z", 3, "letag", "othertag", 1]
                    ]
            }],
            "statement_id": 0
    }]
 }
*/


@Serial
@("query database with data")
unittest {
    import std.algorithm: map;

    manage(influxURL, "DROP DATABASE test_vibe_db");
    wait;
    manage(influxURL, "CREATE DATABASE test_vibe_db");
    wait;
    scope(exit) {
        manage(influxURL, "DROP DATABASE test_vibe_db");
        wait;
    }

    write(influxURL, "test_vibe_db", "foo,tag1=letag,tag2=othertag value=1,othervalue=3");
    write(influxURL, "test_vibe_db", "foo,tag1=toto,tag2=titi value=2,othervalue=4 1434055562000000000");
    wait;

    {
        const json = query(influxURL, "test_vibe_db", "SELECT * from foo").parseJSON;
        const result = json.object["results"].array[0].object;
        const table = result["series"].array[0].object;
        table["columns"].array.map!(a => a.str).shouldBeSameSetAs(
            ["time", "othervalue", "tag1", "tag2", "value"]);
        table["name"].str.shouldEqual("foo");
        table["values"].array.length.shouldEqual(2);
    }
}


// @Serial
// @("Database api")
// unittest {

//     import influxdb.api: Database, Measurement;

//     const database = Database(influxURL, "myspecialDB");
//     scope(exit) database.drop;

//     database.insert(Measurement("cpu", ["tag1": "foo"], ["temperature": "42"]));
//     database.insert(Measurement("cpu", ["tag1": "foo"], ["temperature": "68"]));

//     {
//         const json = database.query("SELECT * from cpu").parseJSON;
//         const result = json.object["results"].array[0].object;
//         const table = result["series"].array[0].object;
//         table["values"].array.length.shouldEqual(2);
//     }

//     {
//         const json = database.query("SELECT * from cpu WHERE temperature > 50").parseJSON;
//         const result = json.object["results"].array[0].object;
//         const table = result["series"].array[0].object;
//         table["values"].array.length.shouldEqual(1);
//     }
// }

private void wait() {
    import core.thread;
    Thread.sleep(10.msecs);
}
