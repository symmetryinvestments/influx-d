module integration.tests;

import unit_threaded;

private string[] shouldSucceed(in string[] cmd, in string file = __FILE__, in size_t line = __LINE__) {
    import std.process: execute;
    import std.conv: text;
    import std.string: splitLines, join;
    import std.algorithm: find, canFind, startsWith, endsWith;
    import std.array: empty;
    import std.json: parseJSON;

    const ret = execute(cmd);
    if(ret.status != 0)
        throw new UnitTestException([text("Could not execute '", cmd.join(" "), "':")] ~
                                    ret.output.splitLines, file, line);

    if(!ret.output.splitLines.canFind!(a => a.canFind("HTTP/1.1 200")) &&
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

enum influxURL = "http://localhost:8086";

private string[] curlPost(in string arg) {
    return ["curl", "-i", "-XPOST", influxURL ~ `/query`, "--data-urlencode",
            `q=` ~ arg];
}

private string[] curlGet(in string arg) {
    return ["curl", "-G", influxURL ~ "/query?pretty=true", "--data-urlencode", "db=testdb",
            "--data-urlencode", `q=` ~ arg];
}

@Serial
@("Create and drop")
unittest {
    curlPost("CREATE DATABASE testdb").shouldSucceed;
    curlPost("DROP DATABASE testdb").shouldSucceed;
}

@Serial
@("Nonsense query")
unittest {
    curlPost("FOO DATABASE testdb").shouldFail;
}

@Serial
@("Query empty database")
unittest {
    import std.string: join;
    import std.json: parseJSON;
    import std.algorithm: find;

    // in case there's still data there, delete the DB
    curlPost("DROP DATABASE testdb").shouldSucceed;
    curlPost("CREATE DATABASE testdb").shouldSucceed;
    scope(exit) curlPost("DROP DATABASE testdb").shouldSucceed;

    const lines = curlGet("SELECT * from foo").shouldSucceed;
    const json = lines.join(" ").find("{").parseJSON;
    json.toString.shouldEqual(`{"results":[{"statement_id":0}]}`);
}


@Serial
@("Query database")
unittest {
    import std.string: join;
    import std.json: parseJSON;
    import std.algorithm: find;

    // in case there's still data there, delete the DB
    curlPost("DROP DATABASE testdb").shouldSucceed;
    curlPost("CREATE DATABASE testdb").shouldSucceed;
    scope(exit) curlPost("DROP DATABASE testdb").shouldSucceed;

    const lines = curlGet("SELECT * from foo").shouldSucceed;
    const json = lines.join(" ").find("{").parseJSON;
    json.toString.shouldEqual(`{"results":[{"statement_id":0}]}`);
}
