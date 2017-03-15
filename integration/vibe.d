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
    manage(influxURL, "CREATE DATABASE test_vibe_db");
    manage(influxURL, "DROP DATABASE test_vibe_db");
}

private void query(in string url, in string db) {

}

@Serial
@("Query empty database")
unittest {
    manage(influxURL, "DROP DATABASE test_vibe_db");
    manage(influxURL, "CREATE DATABASE test_vibe_db");
    scope(exit) manage(influxURL, "DROP DATABASE test_vibe_db");

    const json = vibeGet(influxURL, "test_vibe_db", "SELECT * from foo");
    json.object["results"].array.length.shouldEqual(1);
    const result = json.object["results"].array[0];
    result.object.keys.shouldBeSameSetAs(["statement_id"]);
}


private void vibePostQuery(in string url, in string str, in string file = __FILE__, in size_t line = __LINE__) {
    requestHTTP(url ~ "/query",
                (scope req){
                    req.method = HTTPMethod.POST;
                    req.contentType= "application/x-www-form-urlencoded";
                    auto body_ = str.urlEncode;
                    writelnUt(body_);
                    req.writeBody(cast(ubyte[])body_);
                },
                (scope res){
                    if(res.statusCode != 200)
                        throw new UnitTestException(res.bodyReader.readAllUTF8);
                }
        );

}

private JSONValue vibeGet(in string url, in string db, in string arg,
                          in string file = __FILE__, in size_t line = __LINE__) {

    import std.algorithm: map;
    import std.array: array, join;
    import std.range: chain;
    import std.json: parseJSON;

    string ret;

    requestHTTP(url ~ "/query?" ~ ["db=" ~ db, "q=" ~ arg].map!urlEncode.array.join("&"),
                (scope req){
                },
                (scope res){
                    if(res.statusCode != 200)
                        throw new UnitTestException(res.bodyReader.readAllUTF8);

                    ret = res.bodyReader.readAllUTF8;
                }
        );

    return ret.parseJSON;
}


string urlEncode(in string str) {
    import vibe.textfilter.urlencode: filterURLEncode;
    import std.array: appender;

    auto output = appender!(char[]);
    const allowedChars = "=";
    filterURLEncode(output, str, allowedChars);
    return cast(string)output.data;
}
