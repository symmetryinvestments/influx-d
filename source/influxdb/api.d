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
import std.datetime: Date, DateTime, SysTime, UTC;

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

        debug {
            (() @trusted => stderr)()
                .writeln("Could not convert ", time, " due to a Phobos bug, reducing precision");
        }

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

    version(Have_mir_algorithm)
    {
        import mir.timeseries: Series;
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
        Series!(TimeIterator, kind, [1], Iterator) series1,
        string[string] commonTags = null,
    ) const
    {
        import mir.timeseries: series;
        import mir.ndslice.topology: repeat, unpack, universal;
        import mir.ndslice.dynamic: transposed;

        return this.insert(
            measurementName,
            [columnName],
            series1.time.series(series1.data.repeat(1).unpack.universal.transposed),
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
        Series!(TimeIterator, kind, [2], Iterator) series,
        string[string] commonTags = null,
    ) const
    {
        alias Time = typeof(series.front.time);
        alias Data = DeepElementType!(typeof(series.front.data));

        import std.traits: isSomeString;
        import std.exception: assumeUnique, enforce;
        import std.array: appender;
        import std.format: FormatSpec, formatValue;

        enforce(measurementName.length);
        enforce(columnNames.length == series.length!1, "columnNames.length should be equal to series.length!1");

        if (series.length == 0)
        {
            return;
        }

        FormatSpec!char fmt;
        auto app = appender!(const(char)[]);

        // name output
        app.put(measurementName);
        // tags output
        if (commonTags.length)
        {
            app.put(",");
            aaFormat(&app.put!(const(char)[]), commonTags);
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
            foreach(j, key; columnNames)
            {
                auto value = observation.data[j];
                if (j)
                    app.put(",");
                app.formatValue(key, fmt);
                app.put("=");
                static if(isSomeString!Data)
                {
                    app.put(`"`);
                    app.formatValue(value, fmt);
                    app.put(`"`);
                }
                else
                {
                    app.formatValue(value, fmt);
                }
            }

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
                app.formatValue(timestamp, fmt);
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
    import std.traits : Unqual;

    string name;
    string[string] tags;
    InfluxValue[string] fields;
    long timestamp;

    this(T)(string name,
         T[string] fields,
         SysTime time = SysTime.fromUnixTime(0))
    @safe
    if (is(Unqual!T == string) || is(Unqual!T == InfluxValue)) {
        string[string] tags;
        this(name, tags, fields, time);
    }

    this(T)(string name,
         string[string] tags,
         T[string] fields,
         SysTime time = SysTime.fromUnixTime(0))
    @safe // impure due to SysTime.fracSecs
    if (is(Unqual!T == string) || is(Unqual!T == InfluxValue)) {

        import std.datetime: nsecs;

        this.name = name;
        this.tags = tags;

        static if (is(Unqual!T == string)) {
            import std.typecons : Nullable;
            InfluxValue[string] ifields;
            () @trusted {
                foreach(element; fields.byKeyValue) {
                    ifields[element.key] = InfluxValue(element.value, Nullable!(InfluxValue.Type).init);
                }
            }();
            this.fields = ifields;
        }
        else this.fields = fields;
        // InfluxDB uses UNIX time in _nanoseconds_
        // stdTime is in hnsecs
        //this.timestamp = time.stdTime / 100;
        this.timestamp = (time.toUnixTime!long * 1_000_000_000 + time.fracSecs.total!"nsecs");
    }

    void toString(Dg)(scope Dg dg) const {

        import std.format: FormatSpec, formatValue;
        import std.typecons: Yes;

        FormatSpec!char fmt;
        dg.escape(`, `).formatValue(name, fmt);
        if (tags.length) {
            dg.formatValue(',', fmt);
            dg.aaFormat(tags);
        }
        dg.formatValue(' ', fmt);
        dg.aaFormat(fields, Yes.quoteStrings);
        if(timestamp != 0) {
            dg.formatValue(' ', fmt);
            dg.formatValue(timestamp, fmt);
        }
    }

    static if (__VERSION__ < 2072) {
        string toString() @safe const {
            import std.array : appender;
            auto res = appender!string;
            toString(res);
            return res.data;
        }
    }
    else {
        deprecated("Use std.conv.to!string instead.")
        string toString()() const {
            import std.conv: to;
            return this.to!string;
        }
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
            dg.formatValue(',', fmt);
        dg.escape(`,= `).formatValue(key, fmt);
        dg.formatValue('=', fmt);
        if(quoteStrings && valueIsString(value)) {
            dg.formatValue('"', fmt);
            dg.escape('"').formatValue(value, fmt);
            dg.formatValue('"', fmt);
        } else
            dg.escape(`, `).formatValue(value, fmt);
    }
}

private auto escape(Dg)(scope Dg dg, in char[] chars...) {

    struct Escaper(Dg) {
        void put(T)(T val) {
            import std.algorithm : canFind;
            import std.format: FormatSpec, formatValue;

            FormatSpec!char fmt;
            foreach (c; val) {
                if (chars.canFind(c))
                    dg.formatValue('\\', fmt);
                dg.formatValue(c, fmt);
            }
        }

        static if (__VERSION__ < 2072) {
            void putChar(char c) {
                import std.algorithm : canFind;
                import std.format : formattedWrite;

                if (chars.canFind(c))
                    dg.formattedWrite("%s", '\\');
                dg.formattedWrite("%s", c);
            }
        }
    }

    return Escaper!Dg();
}

private auto valueIsString(T)(in T value) {
    static if (is(T == string)) return true;
    else static if (is(T == InfluxValue)) return value.type == InfluxValue.Type.string;
    else static assert(0, format!"Unexpected value type %s"(typeid(T)));
}

private auto guessValueType(string value) @safe pure nothrow @nogc {

    import std.algorithm: all, canFind;
    import std.string : representation;

    static immutable boolValues = ["t", "T", "true", "True", "TRUE", "f", "F", "false", "False", "FALSE"];

    // test for bool values
    if(boolValues.canFind(value)) return InfluxValue.Type.bool_;

    // test for int values
    if(value.length > 0 && value[$ - 1] == 'i') {
        auto tmp = value[0..$-1];
        if (tmp[0] == '-' && tmp.length > 1) tmp = tmp[1..$];
        if (tmp.representation.all!(a => a >= '0' && a <= '9')) return InfluxValue.Type.int_;
    }

    // test for float values
    if (valueIsFloat(value)) return InfluxValue.Type.float_;

    return InfluxValue.Type.string;
}

private bool valueIsFloat(in string value) @safe pure nothrow @nogc {

    if (!value.length) return false;

    int dotidx = -1;
    int eidx = -1;
    foreach(i, c; value) {
        if (c == '+' || c == '-') {
            if (i != 0 && (eidx < 0 || (eidx >= 0 && i-1 != eidx))) return false;
        }
        else if (c == '.') {
            if (dotidx >= 0 || eidx > 0) return false;
            dotidx = cast(int)i;
        }
        else if (c == 'e' || c == 'E') {
            if (i == 0 || eidx > 0 || i+1 == value.length) return false;
            eidx = cast(int)i;
        }
        else if (c < '0' || c > '9') return false;
    }
    return true;
}

///
@("valueIsFloat")
@safe unittest {
    // valid
    "123".valueIsFloat.shouldBeTrue;
    "-123".valueIsFloat.shouldBeTrue;
    "1e1".valueIsFloat.shouldBeTrue;
    "+1.e1".valueIsFloat.shouldBeTrue;
    ".1e1".valueIsFloat.shouldBeTrue;
    "1e+1".valueIsFloat.shouldBeTrue;
    "1.E-1".valueIsFloat.shouldBeTrue;
    "1.2".valueIsFloat.shouldBeTrue;
    "-1.3e+10".valueIsFloat.shouldBeTrue;
    "+1.".valueIsFloat.shouldBeTrue;

    // invalid
    "1a".valueIsFloat.shouldBeFalse;
    "1e.1".valueIsFloat.shouldBeFalse;
    "e1".valueIsFloat.shouldBeFalse;
    "".valueIsFloat.shouldBeFalse;
    "1eE1".valueIsFloat.shouldBeFalse;
    "1..0".valueIsFloat.shouldBeFalse;
    "1.e.1".valueIsFloat.shouldBeFalse;
    "1e12.3".valueIsFloat.shouldBeFalse;
    "1e1+1".valueIsFloat.shouldBeFalse;
    "ee".valueIsFloat.shouldBeFalse;
    "1ee1".valueIsFloat.shouldBeFalse;
    "++1".valueIsFloat.shouldBeFalse;
    "1+1".valueIsFloat.shouldBeFalse;
    "1.1.1".valueIsFloat.shouldBeFalse;
    "1+".valueIsFloat.shouldBeFalse;
    "1ě+1".valueIsFloat.shouldBeFalse;
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
                         ["foo": "16i", "bar": "-1i", "str": "-i"],
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

struct InfluxValue {

    import std.typecons : Nullable;

    enum Type { bool_, int_, float_, string }

    union Payload {
        bool b;
        long i;
        double f;
    }

    private {
        Payload _value;
        string _rawString;
        Type _type;
    }

    this(bool v) @safe pure nothrow {
        _value.b = v;
        _type = InfluxValue.Type.bool_;
    }

    this(T)(T v) @safe pure nothrow
    if (is(T == int) || is(T == long)) {
        _value.i = v;
        _type = InfluxValue.Type.int_;
    }

    this(T)(T v) @safe pure nothrow
    if (is(T == float) || is(T == double)) {
        _value.f = v;
        _type = InfluxValue.Type.float_;
    }

    this(string v, Nullable!Type type = Nullable!Type(Type.string)) @safe pure nothrow {
        _rawString = v;
        if (type.isNull) _type = guessValueType(v);
        else _type = type;
    }

    auto type() const @safe pure nothrow {
        return _type;
    }

    void toString(Dg)(scope Dg dg) const {

        import std.format: FormatSpec, formattedWrite, formatValue;

        FormatSpec!char fmt;
        if (_rawString.length) {
            if (_type == Type.int_ && _rawString[$-1] != 'i') dg.formattedWrite("%si", _rawString, fmt);
            else dg.formatValue(_rawString, fmt);
        }
        else {
            final switch (_type) with (Type) {
                case bool_: dg.formatValue(_value.b, fmt); break;
                case int_: dg.formattedWrite("%si", _value.i, fmt); break;
                case float_: dg.formatValue(_value.f, fmt); break;
                case string: assert(0);
            }
        }
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

@("Measurement.to!string InfluxValue long")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    const m = Measurement("cpu",
                          ["foo": InfluxValue(16L)],
                          SysTime.fromUnixTime(7));
    m.to!string.shouldEqualLine(`cpu foo=16i 7000000000`);
}

@("Measurement.to!string InfluxValue float")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    Measurement("cpu",
                ["foo": InfluxValue(16.0f)],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo=16 7000000000`);

    Measurement("cpu",
                ["foo": InfluxValue(16.1)],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo=16.1 7000000000`);
}

@("Measurement.to!string InfluxValue boolean")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    Measurement("cpu",
                ["foo": InfluxValue(true)],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo=true 7000000000`);
}

@("Measurement.to!string InfluxValue string")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    Measurement("cpu",
                ["foo": InfluxValue("bar")],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo="bar" 7000000000`);
}

@("Measurement.to!string InfluxValue string with specified bool value")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;
    import std.typecons : nullable;

    Measurement("cpu",
                ["foo": InfluxValue("true", InfluxValue.Type.bool_.nullable)],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo=true 7000000000`);
}

@("Measurement.to!string InfluxValue string with specified int value")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;
    import std.typecons : nullable;

    Measurement("cpu",
                ["foo": InfluxValue("42", InfluxValue.Type.int_.nullable)],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo=42i 7000000000`);
}

@("Measurement.to!string InfluxValue string with specified postfixed int value")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;
    import std.typecons : nullable;

    Measurement("cpu",
                ["foo": InfluxValue("42i", InfluxValue.Type.int_.nullable)],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo=42i 7000000000`);
}

@("Measurement.to!string InfluxValue string with specified float value")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;
    import std.typecons : nullable;

    Measurement("cpu",
                ["foo": InfluxValue("1.2", InfluxValue.Type.float_.nullable)],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo=1.2 7000000000`);
}

@("Measurement.to!string InfluxValue string with float value")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    Measurement("cpu",
                ["foo": InfluxValue("5E57758")],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo="5E57758" 7000000000`);
}

@("Measurement.to!string InfluxValue string with guessed bool value")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;
    import std.typecons : Nullable;

    Measurement("cpu",
                ["foo": InfluxValue("true", Nullable!(InfluxValue.Type).init)],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo=true 7000000000`);
}

@("Measurement.to!string InfluxValue string with guessed int value")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;
    import std.typecons : Nullable;

    Measurement("cpu",
                ["foo": InfluxValue("42i", Nullable!(InfluxValue.Type).init)],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo=42i 7000000000`);
}

@("Measurement.to!string InfluxValue string with guessed float value")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;
    import std.typecons : Nullable;

    Measurement("cpu",
                ["foo": InfluxValue("1.2", Nullable!(InfluxValue.Type).init)],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo=1.2 7000000000`);
}

@("Measurement.to!string InfluxValue guessed string value")
@safe unittest {
    import std.conv: to;
    import std.datetime: SysTime;

    Measurement("cpu",
                ["foo": InfluxValue("bar")],
                SysTime.fromUnixTime(7))
        .to!string.shouldEqualLine(`cpu foo="bar" 7000000000`);
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
