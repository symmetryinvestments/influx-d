module integration.tests;

import unit_threaded;

private void shouldSucceed(in string[] cmd, in string file = __FILE__, in size_t line = __LINE__) {
    import std.process: execute;
    import std.conv: text;
    import std.string: splitLines, join;
    import std.algorithm: find, canFind;
    import std.array: empty;

    const ret = execute(cmd);
    if(ret.status != 0)
        throw new UnitTestException([text("Could not execute '", cmd.join(" "), "':")] ~
                                    ret.output.splitLines, file, line);

    if(!ret.output.splitLines.canFind!(a => a.canFind("HTTP/1.1 200")))
        throw new UnitTestException(["Bad HTTP response"] ~ ret.output.splitLines, file, line);
}

private void shouldFail(in string[] cmd, in string file = __FILE__, in size_t line = __LINE__) {

    import std.conv: text;

    try {
        shouldSucceed(cmd, file, line);
        fail(text("Command '", cmd, "' was expected to fail but did not:"), file, line);
    } catch(Exception ex) {}
}

private string[] curlCmd(in string arg) {
    return ["curl", "-i", "-XPOST", "http://localhost:8086/query", "--data-urlencode", arg];
}

@("Create and drop")
unittest {
    curlCmd("q=CREATE DATABASE testdb").shouldSucceed;
    curlCmd("q=DROP DATABASE testdb").shouldSucceed;
}

@("Nonsense query")
unittest {
    curlCmd("q=FOO DATABASE testdb").shouldFail;
}
