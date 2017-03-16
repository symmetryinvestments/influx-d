module influxdb.vibe;


struct Database {

    import influxdb.api;

    string url; // e.g. http://localhost:8086
    string db;  // e.g. mydb

    @disable this();
    this(string url, string db) {
        this.url = url;
        this.db = db;

        manage("CREATE DATABASE " ~ db);
    }

    void manage(in string cmd) {
        .manage(url, cmd);
    }

    /**
       Returns a JSON object. The return type is auto to avoid
       the top-level import based on the parsing function.
    */
    auto query(in string query) {
        return .query(url, db, query);
    }

    void insert(in Measurement[] measurements) {
        foreach(ref const m; measurements)
            .write(url, db, m.toString);
    }

    void insert(in Measurement[] measurements...) {
        insert(measurements);
    }

    void drop() {
        manage("DROP DATABASE " ~ db);
    }
}


void manage(in string url, in string str) {
    vibePostQuery(url, "q=" ~ str);
}

/**
   Returns a JSON object. The return type is auto to avoid
   the top-level import based on the parsing function.
 */
auto query(in string url, in string db, in string query) {
    import std.json: parseJSON;
    return vibeGet(url, db, query).parseJSON;
}

void write(in string url, in string db, in string line) {
    vibePostWrite(url, db, line);
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


private string vibeGet(in string url, in string db, in string arg,
                          in string file = __FILE__, in size_t line = __LINE__) {

    import std.algorithm: map;
    import std.array: array, join;
    import std.range: chain;
    import std.json: parseJSON;

    const fullUrl = url ~ "/query?" ~ ["db=" ~ db, "q=" ~ arg].map!urlEncode.array.join("&");
    return vibeHttpRequest(fullUrl, [], file, line);
}

private string vibeHttpRequest(in string url,
                               in ubyte[] data,
                               in string file = __FILE__,
                               in size_t line = __LINE__) {

    import vibe.http.client: requestHTTP, HTTPMethod;
    import vibe.stream.operations: readAllUTF8;

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
                        throw new Exception(ret, file, line);
                }
        );

    return ret;

}

string urlEncode(in string str) {
    import vibe.textfilter.urlencode: filterURLEncode;
    import std.array: appender;

    auto output = appender!(char[]);
    const allowedChars = "=";
    filterURLEncode(output, str, allowedChars);
    return cast(string)output.data;
}
