/**
 This module implements integration tests for InfluxDB. As such, they record in
 code the assumptions made with regards to the HTTP API. Given that these tests
 pass, the unit tests are sufficient to guarantee correct behaviour.

 These tests can be run with `dub run -c integration` and require a running
 instance of InfluxDB on localhost:8086. On systems with systemd, install
 InfluxDB (as appropriate for the Linux distribution) and start it with
 `systemctl start influxdb`.

 If these tests fail, nothing else in this repository will work.
 */
module integration.curl;

import unit_threaded;


enum influxURL = "http://localhost:8086";


@Serial
@("Create and drop")
unittest {
    curlPostQuery("CREATE DATABASE testdb").shouldSucceed;
    curlPostQuery("DROP DATABASE testdb").shouldSucceed;
}

@Serial
@("Nonsense query")
unittest {
    curlPostQuery("FOO DATABASE testdb").shouldFail;
}

@Serial
@("Query empty database")
unittest {
    import std.string: join;
    import std.json: parseJSON;
    import std.algorithm: find;

    // in case there's still data there, delete the DB
    curlPostQuery("DROP DATABASE testdb").shouldSucceed;
    curlPostQuery("CREATE DATABASE testdb").shouldSucceed;
    scope(exit) curlPostQuery("DROP DATABASE testdb").shouldSucceed;

    const lines = curlGet("SELECT * from foo").shouldSucceed;
    const json = lines.join(" ").find("{").parseJSON;
    json.toString.shouldEqual(`{"results":[{"statement_id":0}]}`);
}


@Serial
@("Query database with data")
unittest {
    import std.string: join;
    import std.json: parseJSON;
    import std.algorithm: find, map;

    // in case there's still data there, delete the DB
    curlPostQuery("DROP DATABASE testdb").shouldSucceed;
    curlPostQuery("CREATE DATABASE testdb").shouldSucceed;
    scope(exit) curlPostQuery("DROP DATABASE testdb").shouldSucceed;

    curlPostWrite("foo,tag1=letag,tag2=othertag value=1,othervalue=3").shouldSucceed;
    curlPostWrite("foo,tag1=toto,tag2=titi value=2,othervalue=4 1434055562000000000").shouldSucceed;

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

    {
        const lines = curlGet("SELECT * from foo").shouldSucceed;
        const json = lines.join(" ").find("{").parseJSON;
        const result = json.object["results"].array[0].object;
        const point = result["series"].array[0].object;
        point["columns"].array.map!(a => a.str).shouldBeSameSetAs(
            ["time", "othervalue", "tag1", "tag2", "value"]);
        point["name"].str.shouldEqual("foo");
        point["values"].array.length.shouldEqual(2);
    }

    {
        const lines = curlGet("SELECT value from foo WHERE value > 1").shouldSucceed;
        const json = lines.join(" ").find("{").parseJSON;
        const result = json.object["results"].array[0].object;
        const point = result["series"].array[0].object;
        point["values"].array.length.shouldEqual(1);
    }

    {
        const lines = curlGet("SELECT othervalue from foo WHERE othervalue > 42").shouldSucceed;
        const json = lines.join(" ").find("{").parseJSON;
        const result = json.object["results"].array[0];
        // no result in this case, no data with othervalue > 42
        json.object["results"].array[0].toString.shouldEqual(`{"statement_id":0}`);
    }
}

private string[] curlPostQuery(in string arg) {
    return ["curl", "-i", "-XPOST", influxURL ~ `/query`, "--data-urlencode",
            `q=` ~ arg];
}

private string[] curlPostWrite(in string arg) {
    return ["curl", "-i", "-XPOST", influxURL ~ `/write?db=testdb`, "--data-binary", arg];
}

private string[] curlGet(in string arg) {
    return ["curl", "-G", influxURL ~ "/query?pretty=true", "--data-urlencode", "db=testdb",
            "--data-urlencode", `q=` ~ arg];
}


private string[] shouldSucceed(in string[] cmd, in string file = __FILE__, in size_t line = __LINE__) {
    import std.process: execute;
    import std.conv: text;
    import std.string: splitLines, join;
    import std.algorithm: find, canFind, startsWith, endsWith;
    import std.array: empty;
    import std.json: parseJSON;

    writelnUt(cmd.join(" "));

    const ret = execute(cmd);
    if(ret.status != 0)
        throw new UnitTestException([text("Could not execute '", cmd.join(" "), "':")] ~
                                    ret.output.splitLines, file, line);

    if(!ret.output.splitLines.canFind!(a => a.canFind("HTTP/1.1 20")) &&
       !ret.output.canFind(`"results"`))
        throw new UnitTestException([text("Bad HTTP response for '", cmd.join(" "), "':")]
                                    ~ ("first: " ~ ret.output[0] ~ " last: " ~ ret.output[$-1])
                                    ~
                                    ret.output.splitLines, file, line);

    return ret.output.splitLines;
}

private void shouldFail(in string[] cmd, in string file = __FILE__, in size_t line = __LINE__) {

    import std.conv: text;

    try {
        shouldSucceed(cmd, file, line);
        fail(text("Command '", cmd, "' was expected to fail but did not:"), file, line);
    } catch(Exception ex) {}
}
