/**
    This module implements a convenience wrapper API for influx.

    Authors: Atila Neves (Kaleidic Associates Advisory Limited)

    Generated documentation:
        http://influxdb.code.kaleidic.io/influxdb.html

*/

module influxdb.api;

version(unittest)
    import unit_threaded;
else
    struct Values { this(string[]...) { } }

static import influxdb.vibe;
import std.typecons: Flag, No;
import std.datetime: DateTime, SysTime;

/++
Params:
    time = Influx-db time string
Returns:
    SysTime
+/
SysTime influxSysTime(string time) @safe
{
    import std.datetime: SysTime, DateTimeException;

    try {
        return SysTime.fromISOExtString(time);
    } catch(DateTimeException ex) {
        // see https://issues.dlang.org/show_bug.cgi?id=16053
        import std.stdio: stderr;
        import std.algorithm: countUntil;

        debug stderr.writeln("Could not convert ", time, " due to a Phobos bug, reducing precision");

        // find where the fractional part starts
        auto dotIndex = time.countUntil(".");
        if(dotIndex < 0)
            dotIndex = time.countUntil(",");
        if(dotIndex < 0)
            throw ex;


        const firstNonDigitIndex = time[dotIndex + 1 .. $].countUntil!(a => a < '0' || a > '9') + dotIndex + 1;
        if(firstNonDigitIndex < 0) throw ex;

        const lastDigitIndex = firstNonDigitIndex - 1;

        foreach(i; 0 .. 4) {
            // try to cut out a number from the fraction
            // inaccurate but better than throwing an exception
            const timeStr =
                time[0 .. lastDigitIndex - i] ~
                time[firstNonDigitIndex .. $];
            try
                return SysTime.fromISOExtString(timeStr);
            catch(DateTimeException _) {}
        }

        throw ex;
    }
}

///
alias Database = DatabaseImpl!(influxdb.vibe.manage, influxdb.vibe.query, influxdb.vibe.write);

/**
    Holds information about the database name and URL, forwards
    it to the implemetation functions for managing, querying and
    writing to the DB
*/
struct DatabaseImpl(alias manageFunc, alias queryFunc, alias writeFunc) {

    import influxdb.api;

    string url; // e.g. http://localhost:8086
    string db;  // e.g. mydb

    @disable this();

    this(string url, string db) {
        this.url = url;
        this.db = db;

        manage("CREATE DATABASE " ~ db);
    }

    /**
       Sends management commands to the DB (CREATE, DROP).
       The parameter must be the full command (e.g. "DROP DATABASE mydb")
     */
    void manage(in string cmd) const {
        manageFunc(url, cmd);
    }

    /**
       Queries the DB. The query must be a full InfluxDB query
       (e.g. "SELECT * FROM foo")
     */
    Response query(in string query) @trusted const { // deserialize is @system
        import asdf: deserialize;
        return queryFunc(url, db, query).deserialize!Response;
    }

    /**
       Insert data into the DB.
     */
    void insert(in Measurement[] measurements) const {
        import std.format: format;

        if(measurements.length == 0) return;

        static if (__VERSION__ >= 2074)
            writeFunc(url, db, format!"%(%s\n%)"(measurements));
        else
            writeFunc(url, db, format("%(%s\n%)", measurements));
    }

    /**
       Insert data into the DB.
     */
    void insert(in Measurement[] measurements...) const {
        insert(measurements);
    }

    /**
      Delete this DB
     */
    void drop() const {
        manage("DROP DATABASE " ~ db);
    }
}

///
@("Database")
@safe unittest { // not pure because of asdf.deserialize

    string[string][] manages;
    string[string][] queries;
    string[string][] writes;

    alias TestDatabase = DatabaseImpl!(
        (url, cmd) => manages ~= ["url": url, "cmd": cmd], // manage
        (url, db, query) { // query
            queries ~= ["url": url, "db": db, "query": query];
            return
            `{
                 "results": [{
                     "series": [{
                             "columns": ["time", "othervalue", "tag1", "tag2", "value"],
                             "name": "lename",
                             "values": [
                                     ["2015-06-11T20:46:02Z", 4, "toto", "titi", 2],
                                     ["2017-03-14T23:15:01.06282785Z", 3, "letag", "othertag", 1]
                             ]
                     }],
                     "statement_id": 33
                 }]
             }`;
        },
        (url, db, line) => writes ~= ["url": url, "db": db, "line": line]
    );

    manages.shouldBeEmpty;
    const database = TestDatabase("http://db.com", "testdb");
    manages.shouldEqual([["url": "http://db.com", "cmd": "CREATE DATABASE testdb"]]);

    writes.shouldBeEmpty;
    database.insert(Measurement("cpu", ["tag1": "foo"], ["temperature": "42"]));
    writes.shouldEqual([["url": "http://db.com", "db": "testdb",
                         "line": "cpu,tag1=foo temperature=42"]]);

    queries.shouldBeEmpty;
    const response = database.query("SELECT * from foo");
    queries.shouldEqual([["url": "http://db.com", "db": "testdb", "query": "SELECT * from foo"]]);

    response.results.length.shouldEqual(1);
    response.results[0].statement_id.shouldEqual(33);
    response.results[0].series.length.shouldEqual(1);
    const series = response.results[0].series[0];
    series.shouldEqual(
        MeasurementSeries(
            "lename", //name
            ["time", "othervalue", "tag1", "tag2", "value"], //columns
            //values
            [
                ["2015-06-11T20:46:02Z", "4", "toto", "titi", "2"],
                ["2017-03-14T23:15:01.06282785Z", "3", "letag", "othertag", "1"],
            ]
        )
    );
}

///
@("insert")
@safe unittest {

    string[] lines;

    alias TestDatabase = DatabaseImpl!(
        (url, cmd) { }, // manage
        (url, db, query) => `{}`, // query
        (url, db, line) => lines ~= line // write
    );

    const database = TestDatabase("http://db.com", "testdb");
    database.insert(
        Measurement("cpu", ["index": "1"], ["temperature": "42"]),
        Measurement("cpu", ["index": "2"], ["temperature": "42"]),
        Measurement("cpu", ["index": "2"], ["temperature": "42"]),
    );

    () @trusted {
        lines.shouldEqual(
            [
                "cpu,index=1 temperature=42\ncpu,index=2 temperature=42\ncpu,index=2 temperature=42",
            ]
        );
    }();
}


@("insert with no measurements does nothing")
@safe unittest {

    string[] lines;

    alias TestDatabase = DatabaseImpl!(
        (url, cmd) { }, // manage
        (url, db, query) => `{}`, // query
        (url, db, line) => lines ~= line // write
    );

    const database = TestDatabase("http://db.com", "testdb");
    Measurement[] measurements;
    database.insert(measurements);
    lines.shouldBeEmpty;
}


/**
   An InfluxDB measurement
 */
struct Measurement {

    import std.datetime: SysTime;

    string name;
    string[string] tags;
    string[string] fields;
    long timestamp;

    this(string name,
         string[string] fields,
         SysTime time = SysTime.fromUnixTime(0))
    @safe nothrow
    {
        string[string] tags;
        this(name, tags, fields, time);
    }

    this(string name,
         string[string] tags,
         string[string] fields,
         SysTime time = SysTime.fromUnixTime(0))
    @safe nothrow { // impure due to SysTime.fracSecs

        import std.datetime: nsecs;

        this.name = name;
        this.tags = tags;
        this.fields = fields;
        // InfluxDB uses UNIX time in _nanoseconds_
        // stdTime is in hnsecs
        //this.timestamp = time.stdTime / 100;
        this.timestamp = (time.toUnixTime!long * 1_000_000_000 + time.fracSecs.total!"nsecs");
    }

    this(string name,
         InfluxValue[string] fields,
         SysTime time = SysTime.fromUnixTime(0))
    @safe {
        string[string] tags;
        this(name, tags, fields, time);
    }

    this(string name,
         string[string] tags,
         InfluxValue[string] fields,
         SysTime time = SysTime.fromUnixTime(0))
    @safe {

        import std.conv: to;

        string[string] stringFields;

        () @trusted {
            foreach(element; fields.byKeyValue) {
                stringFields[element.key] = element.value.to!string;
            }
        }();

        this(name, tags, stringFields, time);
    }

    void toString(Dg)(Dg dg) const {

        import std.typecons: Yes;

        dg(name);
        if (tags.length)
        {
            dg(",");
            dg.aaFormat(tags);
        }
        dg(" ");
        dg.aaFormat(fields, Yes.quoteStrings);
        if(timestamp != 0)
        {
            dg(" ");
            import std.format: FormatSpec, formatValue;
            FormatSpec!char fmt;
            dg.formatValue(timestamp, fmt);
        }
    }

    deprecated("Use std.conv.to!string instead.")
    string toString()() {
        import std.conv: to;
        return this.to!string;
    }
}

private void aaFormat(Dg, T : K[V], K, V)
                     (scope Dg dg, scope T aa, in Flag!"quoteStrings" quoteStrings = No.quoteStrings)
{
    import std.format: FormatSpec, formatValue;
    size_t i;
    FormatSpec!char fmt;
    foreach(key, value; aa)
    {
        if (i++)
            dg(",");
        dg.formatValue(key, fmt);
        dg("=");
        if(quoteStrings && valueIsString(value)) {
            dg.formatValue(`"`, fmt);
            dg.formatValue(value, fmt);
            dg.formatValue(`"`, fmt);
        } else
            dg.formatValue(value, fmt);
    }
}


private bool valueIsString(in string value) @safe pure nothrow {
    import std.conv: to;
    import std.algorithm: canFind;

    bool ret = true;

    try {
        value.to!double;
        return false;
    } catch(Exception _) {
    }

    try {
        value.to!bool;
        return false;
    } catch(Exception _) {
    }

    if(["t", "T", "f", "F"].canFind(value))
        return false;

    if(value.length > 0 && value[$ - 1] == 'i') {
        try {
            value[0 .. $ - 1].to!int;
            return false;
        } catch(Exception _) {
        }
    }

    return true;
}

///
@("Measurement.to!string no timestamp")
@safe unittest {
    import std.conv: to;
    {
        auto m = Measurement("cpu",
                             ["tag1": "toto", "tag2": "foo"],
                             ["load": "42", "temperature": "53"]);
        m.to!string.shouldEqualLine("cpu,tag1=toto,tag2=foo load=42,temperature=53");
    }
    {
        auto m = Measurement("thingie",
                             ["foo": "bar"],
                             ["value": "7"]);
        m.to!string.shouldEqualLine("thingie,foo=bar value=7");
    }
}

///
@("Measurement.to!string no timestamp no tags")
@safe unittest {
    import std.conv: to;
    auto m = Measurement("cpu",
                         ["load": "42", "temperature": "53"]);
    m.to!string.shouldEqualLine("cpu load=42,temperature=53");
}

///
@("Measurement.to!string with timestamp")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    auto m = Measurement("cpu",
                         ["tag1": "toto", "tag2": "foo"],
                         ["load": "42", "temperature": "53"],
                         SysTime.fromUnixTime(7));
    m.to!string.shouldEqualLine("cpu,tag1=toto,tag2=foo load=42,temperature=53 7000000000");
}

///
@("Measurement.to!string with timestamp no tags")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    auto m = Measurement("cpu",
                         ["load": "42", "temperature": "53"],
                         SysTime.fromUnixTime(7));
    m.to!string.shouldEqualLine("cpu load=42,temperature=53 7000000000");
}

@("Measurement fraction of a second")
@safe unittest {
    import std.conv: to;
    import std.datetime: DateTime, SysTime, Duration, usecs, nsecs, UTC;
    auto m = Measurement("cpu",
                         ["load": "42", "temperature": "53"],
                         SysTime(DateTime(2017, 2, 1), 300.usecs + 700.nsecs, UTC()));
    m.to!string.shouldEqualLine("cpu load=42,temperature=53 1485907200000300700");
}

@("Measurement.to!string with string")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    auto m = Measurement("cpu",
                         ["foo": "bar"],
                         SysTime.fromUnixTime(7));
    m.to!string.shouldEqualLine(`cpu foo="bar" 7000000000`);
}

@("Measurement.to!string with bool")
@Values("t", "T", "true", "True", "TRUE", "f", "F", "false", "False", "FALSE")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    const value = getValue!string;
    auto m = Measurement("cpu",
                         ["foo": value],
                         SysTime.fromUnixTime(7));
    m.to!string.shouldEqualLine(`cpu foo=` ~ value ~ ` 7000000000`);
}

@("Measurement.to!string with int")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    auto m = Measurement("cpu",
                         ["foo": "16i"],
                         SysTime.fromUnixTime(7));
    m.to!string.shouldEqualLine(`cpu foo=16i 7000000000`);
}

struct InfluxValue {

    string value;

    this(int v) @safe pure {
        import std.conv: to;
        value = v.to!string ~ "i";
    }

    this(float v) @safe {
        import std.conv: to;
        value = v.to!string;
    }

    string toString() @safe pure nothrow @nogc {
        return value;
    }
}

@("Measurement.to!string InfluxValue int")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    const m = Measurement("cpu",
                          ["foo": InfluxValue(16)],
                          SysTime.fromUnixTime(7));
    m.to!string.shouldEqualLine(`cpu foo=16i 7000000000`);
}

@("Measurement.to!string InfluxValue float")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    Measurement("cpu",
                ["foo": InfluxValue(16.0)],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo=16 7000000000`);

    Measurement("cpu",
                ["foo": InfluxValue(16.1)],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo=16.1 7000000000`);
}


/**
   A query response
 */
struct Response {
    Result[] results;
}

/**
   A result of a query
 */
struct Result {
    MeasurementSeries[] series;
    int statement_id;
}

/**
   Data for one measurement
 */
struct MeasurementSeries {

    import asdf: serializationIgnoreIn, Asdf;

    string name;
    string[] columns;
    @serializationIgnoreIn string[][] values;

    static struct Rows {
        import std.range: Transversal, TransverseOptions;

        const string[] columns;
        const(string[])[] rows;

        static struct Row {

            import std.datetime: SysTime;

            const string[] columnNames;
            const string[] columnValues;

            string opIndex(in string key) @safe pure const {
                import std.algorithm: countUntil;
                return columnValues[columnNames.countUntil(key)];
            }

            string get(string key, string defaultValue) @safe pure const {
                import std.algorithm: countUntil;
                auto i = columnNames.countUntil(key);
                return (i==-1) ? defaultValue : columnValues[i];
            }

            SysTime time() @safe const {
                return influxSysTime(this["time"]);
            }

            void toString(Dg)(scope Dg dg) const {
                dg("Row(");
                foreach(i, value; columnValues) {
                    if (i)
                        dg(", ");
                    dg(columnNames[i]);
                    dg(": ");
                    dg(value);
                }
                dg(")");
            }

            deprecated("Use std.conv.to!string instead.")
            string toString()() {
                import std.conv: to;
                return this.to!string;
            }
        }

        Row opIndex(in size_t i) @safe pure const nothrow {
            return Row(columns, rows[i]);
        }

        /++
        Params:
            key = column name
        Returns:
            Column as range access range of strings.
        Throws:
            Exception, if key was not found.
        +/
        Transversal!(const(string[])[], TransverseOptions.assumeNotJagged)
        opIndex(in string key) @safe pure const {
            import std.algorithm: countUntil;
            size_t idx = columns.countUntil(key);
            if (idx >= columns.length)
                throw new Exception("Unknown key " ~ key);
            return typeof(return)(rows, idx);
        }

        size_t length() @safe pure const nothrow { return rows.length; }

        void popFront() @safe pure nothrow {
            rows = rows[1 .. $];
        }

        Row front() @safe pure nothrow {
            return this[0];
        }

        bool empty() @safe pure nothrow const {
            return rows.length == 0;
        }
    }

    inout(Rows) rows() @safe pure nothrow inout {
        return inout(Rows)(columns, values);
    }

    void finalizeDeserialization(Asdf data) {
        import std.algorithm: map, count;
        import std.array: uninitializedArray;
        auto rows = data["values"].byElement.map!"a.byElement";
        // count is fast for Asdf
        values = uninitializedArray!(string[][])(rows.count, columns.length);
        foreach(value; values)
        {
            auto row = rows.front;
            assert(row.count == columns.length);
            foreach (ref e; value)
            {
                // do not allocates data here because of `const`,
                // reuses Asdf data
                e = cast(string) cast(const(char)[]) row.front;
                row.popFront;
            }
            rows.popFront;
        }
    }
}

///
@("MeasurementSeries")
@safe unittest {

    import std.datetime: SysTime, DateTime, UTC;
    import std.array: array;

    auto series = MeasurementSeries("coolness",
                                    ["time", "foo", "bar"],
                                    [
                                        ["2015-06-11T20:46:02Z", "red", "blue"],
                                        ["2013-02-09T12:34:56Z", "green", "yellow"],
                                    ]);

    series.rows[0]["foo"].shouldEqual("red");
    series.rows[0]["time"].shouldEqual("2015-06-11T20:46:02Z");
    series.rows[0].time.shouldEqual(SysTime(DateTime(2015, 06, 11, 20, 46, 2), UTC()));

    series.rows[1]["bar"].shouldEqual("yellow");
    series.rows[1]["time"].shouldEqual("2013-02-09T12:34:56Z");
    series.rows[1].time.shouldEqual(SysTime(DateTime(2013, 2, 9, 12, 34, 56), UTC()));

    series.rows["time"][0].shouldEqual("2015-06-11T20:46:02Z");
    series.rows["bar"][1].shouldEqual("yellow");

    series.rows.array.shouldEqual(
        [
            MeasurementSeries.Rows.Row(["time", "foo", "bar"],
                                       ["2015-06-11T20:46:02Z", "red", "blue"],
                                       ),
            MeasurementSeries.Rows.Row(["time", "foo", "bar"],
                                       ["2013-02-09T12:34:56Z", "green", "yellow"],
                                       ),
        ]
    );
}

///
@("MeasurementSeries.get")
@safe pure unittest {
    auto series = MeasurementSeries("coolness",
                                    ["time", "foo", "bar"],
                                    [["2015-06-11T20:46:02Z", "red", "blue"]]);
    series.rows[0].get("foo", "oops").shouldEqual("red");
    series.rows[0].get("quux", "oops").shouldEqual("oops");
}

///
@("MeasurementSeries.Row.to!string")
@safe pure unittest {
    import std.conv: to;
    auto series = MeasurementSeries("coolness",
                                    ["time", "foo", "bar"],
                                    [["2015-06-11T20:46:02Z", "red", "blue"]]);
    series.rows[0].to!string.shouldEqual("Row(time: 2015-06-11T20:46:02Z, foo: red, bar: blue)");
}

///
@("MeasurementSeries long fraction in ISOExtString")
@safe unittest {

    import std.datetime: SysTime, DateTime, UTC, usecs;
    import std.array: array;

    auto series = MeasurementSeries("coolness",
                                    ["time", "foo", "bar"],
                                    [
                                        ["2017-05-10T14:47:38.82524801Z", "red", "blue"],
                                    ]);

    series.rows[0].time.shouldEqual(SysTime(DateTime(2017, 05, 10, 14, 47, 38), 825248.usecs, UTC()));
}


/**
   Converts a DateTime to a string suitable for use in queries
   e.g. SELECT * FROM foo WHERE time >=
 */
string toInfluxDateTime(in DateTime time) @safe {
    import std.datetime: UTC;
    return toInfluxDateTime(SysTime(time, UTC()));
}

///
@("toInfluxDateTime with DateTime")
unittest {
    DateTime(2017, 2, 1).toInfluxDateTime.shouldEqual("'2017-02-01T00:00:00Z'");
}

/**
   Converts a SysTime to a string suitable for use in queries
   e.g. SELECT * FROM foo WHERE time >=
 */

string toInfluxDateTime(in SysTime time) @safe {
    return "'" ~ time.toISOExtString ~ "'";
}

///
@("toInfluxDateTime with SysTime")
unittest {
    import std.datetime: UTC;
    SysTime(DateTime(2017, 2, 1), UTC()).toInfluxDateTime.shouldEqual("'2017-02-01T00:00:00Z'");
}

version(unittest) {
    /**
    Example:
    The two lines must be equivalent under InfluxDB's line protocol
       Since the tags and fields aren't ordered, a straight comparison
       might yield false errors.
       The timestamp is also taken care of by comparing it to the current timestamp
       and making sure not too much time has passed since then
     */
    void shouldEqualLine(in string actual,
                         in string expected,
                         in string file = __FILE__,
                         in size_t line = __LINE__) @safe pure {

        // reassemble the protocol line with sorted tags and fields
        string sortLine(in string line) {

            import std.string: split, join;
            import std.range: chain;
            import std.algorithm: sort;
            import std.conv: text;

            auto parts = line.split(" ");
            assert(parts.length == 3 || parts.length == 2,
                   text("Illegal number of parts( ", parts.length, ") in ", line));

            auto nameTags = parts[0].split(",");
            const name = nameTags[0];
            auto tags = nameTags[1..$];

            auto fields = parts[1].split(",");

            auto newNameTags = chain([name], sort(tags)).join(",");
            auto newFields = sort(fields).join(",");
            auto newParts = [newNameTags, newFields];
            if(parts.length > 2) newParts ~= parts[2];

            return newParts.join(" ");
        }

        sortLine(actual).shouldEqual(sortLine(expected), file, line);
    }
}
