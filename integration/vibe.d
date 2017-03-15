/**
 This module makes sure that using vibe corresponds in using the HTTP API
 correctly.
 */
module integration.vibe;

import unit_threaded;
import std.json: JSONValue;
import integration.common: influxURL;
import vibe.http.client: requestHTTP, HTTPMethod;
import vibe.stream.operations: readAllUTF8;

private void manage(in string url, in string str) {
    vibePostQuery(url, "q=" ~ str);
}

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

private void query(in string url, in string db) {

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

    const json = vibeGet(influxURL, "test_vibe_db", "SELECT * from foo");
    JSONValue expected;
    JSONValue result;
    result["statement_id"] = JSONValue(0);
    expected["results"] = [result];
    json.shouldEqual(expected);
}

@HiddenTest
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

    vibePostWrite(influxURL, "test_vibe_db", "foo,tag1=letag,tag2=othertag value=1,othervalue=3");
    vibePostWrite(influxURL, "test_vibe_db", "foo,tag1=toto,tag2=titi value=2,othervalue=4 1434055562000000000");
    wait;

    {
        const json = vibeGet(influxURL, "test_vibe_db", "SELECT * from foo");
        const result = json.object["results"].array[0].object;
        const point = result["series"].array[0].object;
        point["columns"].array.map!(a => a.str).shouldBeSameSetAs(
            ["time", "othervalue", "tag1", "tag2", "value"]);
        point["name"].str.shouldEqual("foo");
        point["values"].array.length.shouldEqual(2);
    }

}

private string vibeHttpRequest(in string url,
                               in ubyte[] data,
                               in string file = __FILE__,
                               in size_t line = __LINE__) {

    string ret;

    requestHTTP(url,
                (scope req) {

                    req.contentType= "application/x-www-form-urlencoded";
                    if(data.length) {
                        req.method = HTTPMethod.POST;
                        req.writeBody(data);
                    }

                },
                (scope res) {
                    ret = res.bodyReader.readAllUTF8;
                    if(res.statusCode < 200 || res.statusCode > 299)
                        throw new UnitTestException(ret, file, line);
                }
        );

    return ret;

}

private string vibePostQuery(in string url,
                           in string query,
                           in string file = __FILE__,
                           in size_t line = __LINE__) {
    return vibeHttpRequest(url ~ "/query",
                           cast(ubyte[])query.urlEncode,
                           file,
                           line);
}

private string vibePostWrite(in string url, in string db, in string str,
                           in string file = __FILE__, in size_t line = __LINE__) {
    return vibeHttpRequest(url ~ "/write?db=" ~ db, cast(ubyte[])str, file, line);
}


private JSONValue vibeGet(in string url, in string db, in string arg,
                          in string file = __FILE__, in size_t line = __LINE__) {

    import std.algorithm: map;
    import std.array: array, join;
    import std.range: chain;
    import std.json: parseJSON;

    const fullUrl = url ~ "/query?" ~ ["db=" ~ db, "q=" ~ arg].map!urlEncode.array.join("&");
    const jsonString =  vibeHttpRequest(fullUrl, [], file, line);
    return jsonString.parseJSON;
}


string urlEncode(in string str) {
    import vibe.textfilter.urlencode: filterURLEncode;
    import std.array: appender;

    auto output = appender!(char[]);
    const allowedChars = "=";
    filterURLEncode(output, str, allowedChars);
    return cast(string)output.data;
}

void wait() {
    import core.thread;
    Thread.sleep(10.msecs);
}
