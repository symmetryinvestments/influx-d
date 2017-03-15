/**
 This module makes sure that using vibe corresponds in using the HTTP API
 correctly.
 */
module integration.vibe;

import unit_threaded;
import integration.common: influxURL;
import vibe.http.client: requestHTTP, HTTPMethod;
import vibe.stream.operations: readAllUTF8;

@Serial
@("manage")
unittest {
    manage(influxURL, "DROP DATABASE testdb");
    manage(influxURL, "CREATE DATABASE testdb");
    manage(influxURL, "DROP DATABASE testdb");
}

private void manage(in string url, in string str) {
    vibePostQuery(url, "q=" ~ str);
}

private void vibePostQuery(in string url, in string str, in string file = __FILE__, in size_t line = __LINE__) {
    requestHTTP(url ~ "/query",
                (scope req){
                    req.method = HTTPMethod.POST;
                    req.contentType= "application/x-www-form-urlencoded";
                    auto body_ = str.urlEncode;
                    writelnUt(cast(string)body_);
                    req.writeBody(body_);
                },
                (scope res){
                    if(res.statusCode != 200)
                        throw new UnitTestException(res.bodyReader.readAllUTF8);
                }
        );

}

private ubyte[] urlEncode(in string str) {
    import vibe.textfilter.urlencode: filterURLEncode;
    import std.array: appender;

    auto output = appender!(ubyte[]);
    const allowedChars = "=";
    filterURLEncode(output, str, allowedChars);
    return output.data;
}
