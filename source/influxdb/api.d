/**
    This module implements a convenience wrapper API for influx.

    Authors: Atila Neves (Kaleidic Associates Advisory Limited)

    Generated documentation:
        http://influxdb.code.kaleidic.io/influxdb.html

*/

module influxdb.api;

import mir.algebraic: Variant;
import mir.string_map: StringMap;

version(Test_InfluxD)
    import unit_threaded;
else
    struct Values { this(string[]...) { } }

static import influxdb.vibe;
import std.typecons: Flag, No;
import std.datetime: Date, DateTime, SysTime, UTC;

///
alias Database = DatabaseImpl!(influxdb.vibe.manage, influxdb.vibe.query, influxdb.vibe.write);

/**
    Holds information about the database name and URL, forwards
    it to the implemetation functions for managing, querying and
    writing to the DB
*/
struct DatabaseImpl(alias manageFunc, alias queryFunc, alias writeFunc) {

    import influxdb.api;

    version(Have_mir_algorithm)
    {
        import mir.series: Series;
        import mir.ndslice.slice: DeepElementType, Slice, SliceKind;
    }

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
        import mir.ion.deser.json: deserializeJson;
        return queryFunc(url, db, query).deserializeJson!Response;
    }

    /**
       Insert data into the DB.
     */
    void insert(in Measurement[] measurements) const {
        import std.format: format;

        if(measurements.length == 0) return;

        static if (__VERSION__ >= 2074)
            writeFunc(url, db, () @trusted { return format!"%(%s\n%)"(measurements); }());
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
        Insert Mir times-series with single column into the DB.
        Supported time types are `SysTime`, `DateTime`, `Date`, and `long`.

        See also the example in the `mir-integration` folder.

        Params:
            measurementName = measurement name
            columnName = column name
            series1 = 1D time-series
            commonTags = list of tags to add to each measurement (optional)
    */
    version(Have_mir_algorithm)
    void insert(TimeIterator, SliceKind kind, Iterator)(
        string measurementName,
        string columnName,
        Series!(TimeIterator, Iterator, 1, kind) series1,
        StringMap!string commonTags = null,
    ) const
    {
        import mir.series: series;
        import mir.ndslice.topology: repeat, unpack;
        import mir.ndslice.dynamic: transposed;

        return this.insert(
            measurementName,
            [columnName],
            series1.time.series(series1.data.repeat(1).unpack.transposed),
            commonTags,
        );
    }

    /**
        Insert Mir times-series with multiple columns into the DB.
        Supported time types are `SysTime`, `DateTime`, `Date`, and `long`.

        See also the example in the `mir-integration` folder.

        Params:
            measurementName = measurement name
            columnNames = array of column names
            series = 2D time-series
            commonTags = list of tags to add to each measurement (optional)
    */
    version(Have_mir_algorithm)
    void insert(TimeIterator, SliceKind kind, Iterator)(
        string measurementName,
        in string[] columnNames,
        Series!(TimeIterator, Iterator, 2, kind) series,
        StringMap!string commonTags = null,
    ) const
    {
        alias Time = typeof(series.front.time);
        alias Data = DeepElementType!(typeof(series.front.data));

        import std.traits: isSomeString;
        import std.exception: assumeUnique, enforce;
        import std.array: appender;
        import mir.format: print;
        import mir.ndslice.topology: as;
        import mir.array.allocation: array;
        import std.typecons: Yes;

        enforce(measurementName.length);
        enforce(columnNames.length == series.length!1, "columnNames.length should be equal to series.length!1");

        if (series.length == 0)
        {
            return;
        }

        auto app = appender!(const(char)[]);

        // name output
        app.put(measurementName);
        // tags output
        if (commonTags.length)
        {
            app.put(",");
            aaFormat(app, commonTags);
        }
        app.put(" ");
        // name + tags
        auto head = app.data;
        app = appender!(const(char)[]);
        foreach (i; 0 .. series.length)
        {
            auto observation = series[i];
            if (i)
            {
                app.put("\n");
            }

            app.put(head);

            // values output
            app.aaFormat(StringMap!Data(cast(string[])columnNames, observation.value.lightScope.as!Data.array), Yes.quoteStrings);

            // time output
            static if (is(Time : long))
            {
                long timestamp = observation.time;
            }
            else
            static if (is(Time : SysTime))
            {
                long timestamp = observation.time.toUnixTime!long * 1_000_000_000 + observation.time.fracSecs.total!"nsecs";
            }
            else
            static if (is(Time : DateTime) || is(Time : Date))
            {
                long timestamp = SysTime(observation.time, UTC()).toUnixTime!long * 1_000_000_000;
            }
            else
            {
                static assert(0, "Unsupported timestamp type: " ~ Time.stringof);
            }
            if (timestamp != 0)
            {
                app.put(" ");
                app.print(timestamp);
            }
        }
        writeFunc(url, db, app.data.assumeUnique);
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
@safe unittest { // impure due to SysTime.fracSecs

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
    database.insert(Measurement("cpu", ["tag1": "foo"], ["temperature": 42]));
    writes.shouldEqual([["url": "http://db.com", "db": "testdb",
                        "line": "cpu,tag1=foo temperature=42i"]]);

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
                ["2015-06-11T20:46:02Z".InfluxValue, 4.InfluxValue, "toto".InfluxValue, "titi".InfluxValue, 2.InfluxValue],
                ["2017-03-14T23:15:01.06282785Z".InfluxValue, 3.InfluxValue, "letag".InfluxValue, "othertag".InfluxValue, 1.InfluxValue],
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
        Measurement("cpu", ["index": "1"], ["temperature": 42]),
        Measurement("cpu", ["index": "2"], ["temperature": 42]),
        Measurement("cpu", ["index": "2"], ["temperature": 42]),
    );

    () @trusted {
        lines.shouldEqual(
            [
                "cpu,index=1 temperature=42i\ncpu,index=2 temperature=42i\ncpu,index=2 temperature=42i",
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
    import mir.timestamp;
    import std.datetime: SysTime;
    import std.traits : Unqual;

    string name;
    StringMap!string tags;
    StringMap!InfluxValue fields;
    Timestamp timestamp;


    this(T)
        (string name,
         T[string] fields,
         SysTime time = SysTime.init)
    @safe
    {
        this(name, null, fields, time);
    }

    this(T)
        (string name,
         string[string] tags,
         T[string] fields,
         SysTime time = SysTime.init)
    @safe
    {
        StringMap!InfluxValue ifields;
        foreach(element; fields.byKeyValue) {
            ifields[element.key] = InfluxValue(element.value);
        }
        this(name, tags.StringMap!string, ifields, time);
    }

    this(string name,
        StringMap!string tags,
        StringMap!InfluxValue fields,
        SysTime time = SysTime.init)
    @safe // impure due to SysTime.fracSecs
    {

        import std.datetime: nsecs;

        this.name = name;
        this.tags = tags;

        this.fields = fields;
        if (time != SysTime.init)
            this.timestamp = time;
    }

    void toString(W)(ref W w) const @trusted {

        import mir.format: print;
        import std.typecons: Yes;

        w.escapedPrint(`, `, name);
        if (tags.length) {
            w.put(',');
            w.aaFormat(tags);
        }
        w.put(' ');
        w.aaFormat(fields, Yes.quoteStrings);
        if(timestamp != timestamp.init) {
            w.put(' ');
            w.print(timestamp.toUnixTime * 10 ^^ 9 + timestamp.getFraction!9);
        }
    }

    string toString() @safe const pure {
        import std.array : appender;
        auto res = appender!string;
        toString(res);
        return res.data;
    }
}

private void aaFormat(W, T : StringMap!V, V)
                     (ref W w, const T aa, in Flag!"quoteStrings" quoteStrings = No.quoteStrings)
{
    import mir.format: print;
    foreach(i, key; aa.keys)
    {
        if (i)
            w.put(',');
        w.escapedPrint(`,= `, key);
        w.put('=');
        auto value = aa.values[i];
        import mir.algebraic: match;
        value.match!(
            (string a) {
                if (quoteStrings) {
                    w.put('"');
                    w.escapedPrint(`"`, a);
                    w.put('"');
                } else {
                    w.escapedPrint(`, `, a);
                }
            },
            (long a) {
                w.print(a);
                w.put('i');
            },
            (bool a) {
                w.print(a);
            },
            (a) {
                w.print(a);
            }
        );
    }
}

private auto escapedPrint(W, T)(ref return W w, return const(char)[] chars, T value) {

    static struct Escaper {
        W* w;
        const(char)[] chars;
        void put(char c) {
            import mir.algorithm.iteration: find;
            if (c == '\\' || chars.find!(a => a == c))
                w.put('\\');
            w.put(c);
        }

        void put(scope const(char)[] str) {
            foreach (char c; str)
                put(c);
        }
    }

    auto ew = Escaper(&w, chars);
    import mir.format: print;
    ew.print(value);

}

///
@("Measurement.to!string no timestamp")
@safe unittest {
    import std.conv: to;
    {
        auto m = Measurement("cpu",
                             ["tag1": "toto", "tag2": "foo"],
                             ["load": 42, "temperature": 53]);
        m.to!string.shouldEqualLine("cpu,tag1=toto,tag2=foo load=42i,temperature=53i");
    }
    {
        auto m = Measurement("thingie",
                             ["foo": "bar"],
                             ["value": 7]);
        m.to!string.shouldEqualLine("thingie,foo=bar value=7i");
    }
}

///
@("Measurement.to!string no timestamp no tags")
@safe unittest {
    import std.conv: to;
    auto m = Measurement("cpu",
                         ["load": 42, "temperature": 53]);
    m.to!string.shouldEqualLine("cpu load=42i,temperature=53i");
}

///
@("Measurement.to!string with timestamp")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    auto m = Measurement("cpu",
                         ["tag1": "toto", "tag2": "foo"],
                         ["load": 42, "temperature": 53],
                         SysTime.fromUnixTime(7));
    m.to!string.shouldEqualLine("cpu,tag1=toto,tag2=foo load=42i,temperature=53i 7000000000");
}

///
@("Measurement.to!string with timestamp no tags")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    auto m = Measurement("cpu",
                         ["load": 42, "temperature": 53],
                         SysTime.fromUnixTime(7));
    m.to!string.shouldEqualLine("cpu load=42i,temperature=53i 7000000000");
}

@("Measurement fraction of a second")
@safe unittest {
    import std.conv: to;
    import std.datetime: DateTime, SysTime, Duration, usecs, nsecs, UTC;
    auto m = Measurement("cpu",
                         ["load": 42, "temperature": 53],
                         SysTime(DateTime(2017, 2, 1), 300.usecs + 700.nsecs, UTC()));
    m.to!string.shouldEqualLine("cpu load=42i,temperature=53i 1485907200000300700");
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


@("Measurement.to!string with int")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    auto m = Measurement("cpu",
                         ["foo": 16.InfluxValue, "bar": InfluxValue(-1), "str": "-i".InfluxValue],
                         SysTime.fromUnixTime(7));
    m.to!string.shouldEqualLine(`cpu foo=16i,bar=-1i,str="-i" 7000000000`);
}

@("Measurement.to!string with special characters")
@safe unittest {
    import std.conv: to;

    auto m = Measurement(`cpu "load", test`,
                         ["tag 1": `to"to`, "tag,2": "foo"],
                         ["foo,= ": "a,b", "b,a=r": `a " b`]);
    m.to!string.shouldEqualLine(`cpu\ "load"\,\ test,tag\ 1=to"to,tag\,2=foo foo\,\=\ ="a,b",b\,a\=r="a \" b"`);
}

/**
   A sum type of values that can be stored in influxdb
 */
 alias InfluxValue = Variant!(string, double, long, bool);

@("Measurement.to!string InfluxValue int")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    const m = Measurement("cpu",
                          ["foo": InfluxValue(16)],
                          SysTime.fromUnixTime(7));
    () @trusted { return m.to!string; }().shouldEqualLine(`cpu foo=16i 7000000000`);
}

@("Measurement.to!string InfluxValue long")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    const m = Measurement("cpu",
                          ["foo": InfluxValue(16L)],
                          SysTime.fromUnixTime(7));
    () @trusted { return m.to!string; }().shouldEqualLine(`cpu foo=16i 7000000000`);
}

@("Measurement.to!string InfluxValue float")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    Measurement("cpu",
                ["foo": 16.0f],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo=16.0 7000000000`);

    Measurement("cpu",
                ["foo": 16.1],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo=16.1 7000000000`);
}

@("Measurement.to!string InfluxValue boolean")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    Measurement("cpu",
                ["foo": true],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo=true 7000000000`);
}

@("Measurement.to!string InfluxValue string")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    Measurement("cpu",
                ["foo": "bar"],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo="bar" 7000000000`);
}

@("Measurement.to!string InfluxValue empty string")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    Measurement("cpu",
                ["foo": ""],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo="" 7000000000`);
}

@("Measurement.to!string InfluxValue string escaping")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    Measurement("cpu",
                ["foo": `{"msg":"\"test\""}`],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo="{\"msg\":\"\\\"test\\\"\"}" 7000000000`);
}

@("Measurement.to!string InfluxValue int value")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;
    import std.typecons : Nullable;

    Measurement("cpu",
                ["foo": 42],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo=42i 7000000000`);
}

@("Measurement.to!string InfluxValue float value")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;
    import std.typecons : Nullable;

    Measurement("cpu",
                ["foo": 1.2],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo=1.2 7000000000`);
}

@("Measurement.to!string InfluxValue string with float value")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    Measurement("cpu",
                ["foo": "5E57758"],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo="5E57758" 7000000000`);
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
    import mir.serde: serdeIgnoreIn;
    import mir.ndslice.slice: Slice;

    string name;
    string[] columns;
    Slice!(InfluxValue*, 2) values;

    this(
        string name,
        string[] columns,
        InfluxValue[][] values,
    ) @safe pure {
        import mir.ndslice.fuse : fuse;
        this.name = name;
        this.columns = columns;
        this.values = values.fuse;
    }

    static struct Rows {
        import std.range: Transversal, TransverseOptions;

        const(string)[] columns;
        Slice!(const(InfluxValue)*, 2) rows;

        static struct Row {

            import std.datetime: SysTime;

            const string[] columnNames;
            const InfluxValue[] columnValues;

            InfluxValue opIndex(in string key) @safe pure const {
                import std.algorithm: countUntil;
                return columnValues[columnNames.countUntil(key)];
            }

            InfluxValue get(string key, InfluxValue defaultValue) @safe pure const {
                import std.algorithm: countUntil;
                auto i = columnNames.countUntil(key);
                return (i==-1) ? defaultValue : columnValues[i];
            }

            SysTime time() @safe const {
                import mir.timestamp: Timestamp;
                return cast(SysTime) this["time"].get!string.Timestamp;
            }

            void toString(W)(ref W w) const {
                w.put("Row(");
                foreach(i, value; columnValues) {
                    if (i)
                        w.put(", ");
                    w.put(columnNames[i]);
                    w.put(": ");
                    import mir.format: print;
                    w.print(value);
                }
                w.put(")");
            }

            string toString() @safe const pure {
                import std.array : appender;
                auto res = appender!string;
                toString(res);
                return res.data;
            }
        }

        Row opIndex(in size_t i) @safe pure const nothrow {
            return Row(columns, rows[i].field);
        }

        /++
        Params:
            key = column name
        Returns:
            Column as range access range of strings.
        Throws:
            Exception, if key was not found.
        +/
        auto opIndex(in string key) @safe pure const {
            import std.algorithm: countUntil;
            size_t idx = columns.countUntil(key);
            if (idx >= columns.length)
                throw new Exception("Unknown key " ~ key);
            return rows[0 .. $, idx];
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

    Rows rows() @safe pure nothrow const {
        return Rows(columns, values);
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
                                        ["2015-06-11T20:46:02Z".InfluxValue, "red".InfluxValue, "blue".InfluxValue],
                                        ["2013-02-09T12:34:56Z".InfluxValue, "green".InfluxValue, "yellow".InfluxValue],
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
                                       ["2015-06-11T20:46:02Z".InfluxValue, "red".InfluxValue, "blue".InfluxValue],
                                       ),
            MeasurementSeries.Rows.Row(["time", "foo", "bar"],
                                       ["2013-02-09T12:34:56Z".InfluxValue, "green".InfluxValue, "yellow".InfluxValue],
                                       ),
        ]
    );
}

///
@("MeasurementSeries.get")
@safe pure unittest {
    auto series = MeasurementSeries("coolness",
                                    ["time", "foo", "bar"],
                                    [["2015-06-11T20:46:02Z".InfluxValue, "red".InfluxValue, "blue".InfluxValue]]);
    series.rows[0].get("foo", "oops".InfluxValue).shouldEqual("red");
    series.rows[0].get("quux", "oops".InfluxValue).shouldEqual("oops");
}

///
@("MeasurementSeries.Row.to!string")
@safe pure unittest {
    import std.conv: to;
    auto series = MeasurementSeries("coolness",
                                    ["time", "foo", "bar"],
                                    [["2015-06-11T20:46:02Z".InfluxValue, "red".InfluxValue, "blue".InfluxValue]]);
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
                                        ["2017-05-10T14:47:38.82524801Z".InfluxValue, "red".InfluxValue, "blue".InfluxValue],
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

version(Test_InfluxD) {
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

            import std.algorithm: sort, splitter;
            import std.array : array;
            import std.conv: text;
            import std.range: chain;
            import std.string: join, split;

            bool isval;
            size_t idx;
            auto parts = line.splitter!((a) {
                if (a == ' ' && !isval && (idx == 0 || line[idx-1] != '\\')) {
                    idx++;
                    return true;
                }
                if (a == '"' && idx > 0 && line[idx-1] != '\\' && line[idx-1] == '=')
                    isval = true;
                else if (a == '"' && idx > 0 && isval && line[idx-1] != '\\')
                    isval = false;
                idx++;
                return false;
            }).array;

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
