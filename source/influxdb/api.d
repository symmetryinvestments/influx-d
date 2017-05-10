/**
    This module implements a convenience wrapper API for influx.

    Authors: Atila Neves (Kaleidic Associates Advisory Limited)

    Generated documentation:
        http://influxdb.code.kaleidic.io/influxdb.html

*/

module influxdb.api;

static import influxdb.vibe;
version(unittest) import unit_threaded;

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
        import std.algorithm: map;
        import std.array: array, join;
        writeFunc(url, db, measurements.map!(a => a.toString).array.join("\n"));
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
    @safe nothrow {
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

    string toString() @safe pure const {
        import std.range: chain;
        import std.conv: to;
        import std.array: join;

        // @trusted due to aa.keys
        auto aaToString(in string[string] aa) @trusted {
            import std.algorithm: map;
            return aa.keys.map!(k => k ~ "=" ~ aa[k]);
        }

        const nameTags = chain([name], aaToString(tags)).join(",");
        const fields = aaToString(fields).join(",");

        auto parts = [nameTags.to!string, fields.to!string];
        if(timestamp != 0) parts ~= timestamp.to!string;

        return parts.join(" ");
    }
}

///
@("Measurement.toString no timestamp")
@safe unittest {
    {
        auto m = Measurement("cpu",
                             ["tag1": "toto", "tag2": "foo"],
                             ["load": "42", "temperature": "53"]);
        m.toString.shouldEqualLine("cpu,tag1=toto,tag2=foo load=42,temperature=53");
    }

    {
        auto m = Measurement("thingie",
                             ["foo": "bar"],
                             ["value": "7"]);
        m.toString.shouldEqualLine("thingie,foo=bar value=7");
    }
}

///
@("Measurement.toString no timestamp no tags")
@safe unittest {
    auto m = Measurement("cpu",
                         ["load": "42", "temperature": "53"]);
    m.toString.shouldEqualLine("cpu load=42,temperature=53");
}

///
@("Measurement.toString with timestamp")
@safe unittest {

    import std.datetime: SysTime;

    auto m = Measurement("cpu",
                         ["tag1": "toto", "tag2": "foo"],
                         ["load": "42", "temperature": "53"],
                         SysTime.fromUnixTime(7));
    m.toString.shouldEqualLine("cpu,tag1=toto,tag2=foo load=42,temperature=53 7000000000");
}

///
@("Measurement.toString with timestamp no tags")
@safe unittest {

    import std.datetime: SysTime;

    auto m = Measurement("cpu",
                         ["load": "42", "temperature": "53"],
                         SysTime.fromUnixTime(7));
    m.toString.shouldEqualLine("cpu load=42,temperature=53 7000000000");
}

@("Measurement fraction of a second")
@safe unittest {
    import std.datetime: DateTime, SysTime, Duration, usecs, nsecs, UTC;
    auto m = Measurement("cpu",
                         ["load": "42", "temperature": "53"],
                         SysTime(DateTime(2017, 2, 1), 300.usecs + 700.nsecs, UTC()));
    m.toString.shouldEqualLine("cpu load=42,temperature=53 1485907200000300700");
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
                return SysTime.fromISOExtString(this["time"]);
            }
            string toString() @safe const pure nothrow {

                import std.string: join;

                string[] ret;
                foreach(i, ref value; columnValues) {
                    ret ~= columnNames[i] ~ ": " ~ value;
                }
                return "Row(" ~ ret.join(", ") ~ ")";
            }
        }

        Row opIndex(in size_t i) @safe pure const nothrow {
            return Row(columns, rows[i]);
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
        import std.algorithm: map;
        import std.array: array;

        auto dataValues = data["values"];
        foreach(row; dataValues.byElement) {
            values ~= row.byElement.map!(a => cast(string)a).array;
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
