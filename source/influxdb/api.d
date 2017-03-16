module influxdb.api;

static import influxdb.vibe;
version(unittest) import unit_threaded;

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

    void manage(in string cmd) {
        manageFunc(url, cmd);
    }

    /**
       Returns a JSON object. The return type is auto to avoid
       the top-level import based on the parsing function.
    */
    auto query(in string query) {
        return queryFunc(url, db, query);
    }

    void insert(in Measurement[] measurements) {
        foreach(ref const m; measurements)
            writeFunc(url, db, m.toString);
    }

    void insert(in Measurement[] measurements...) {
        insert(measurements);
    }

    void drop() {
        manage("DROP DATABASE " ~ db);
    }
}

@("Database")
@safe pure unittest {

    string[string][] manages;
    string[string][] queries;
    string[string][] writes;

    alias TestDatabase = DatabaseImpl!(
        (url, cmd) => manages ~= ["url": url, "cmd": cmd],
        (url, db, query) {
            queries ~= ["url": url, "db": db, "query": query];
            return `{}`;
        },
        (url, db, line) => writes ~= ["url": url, "db": db, "line": line]
    );

    manages.shouldBeEmpty;
    auto database = TestDatabase("http://db.com", "testdb");
    manages.shouldEqual([["url": "http://db.com", "cmd": "CREATE DATABASE testdb"]]);

    writes.shouldBeEmpty;
    database.insert(Measurement("cpu", ["tag1": "foo"], ["temperature": "42"]));
    writes.shouldEqual([["url": "http://db.com", "db": "testdb",
                         "line": "cpu,tag1=foo temperature=42"]]);

    queries.shouldBeEmpty;
    database.query("SELECT * from foo");
    queries.shouldEqual([["url": "http://db.com", "db": "testdb", "query": "SELECT * from foo"]]);
}


struct Measurement {

    import std.datetime: SysTime;

    string name;
    string[string] tags;
    string[string] fields;
    long timestamp;

    @disable this();

    this(string name,
         string[string] fields,
         SysTime time = SysTime.fromUnixTime(0))
    @safe pure nothrow {
        string[string] tags;
        this(name, tags, fields);
    }

    this(string name,
         string[string] tags,
         string[string] fields,
         SysTime time = SysTime.fromUnixTime(0))
    @safe pure nothrow {
        this.name = name;
        this.tags = tags;
        this.fields = fields;
        this.timestamp = time.toUnixTime;
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


@("Measurement.toString no timestamp")
@safe pure unittest {
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

@("Measurement.toString no timestamp no tags")
@safe pure unittest {
    auto m = Measurement("cpu",
                         ["load": "42", "temperature": "53"]);
    m.toString.shouldEqualLine("cpu load=42,temperature=53");
}

@("Measurement.toString with timestamp")
@safe pure unittest {

    import std.datetime: SysTime;

    auto m = Measurement("cpu",
                         ["tag1": "toto", "tag2": "foo"],
                         ["load": "42", "temperature": "53"],
                         SysTime.fromUnixTime(7));
    m.toString.shouldEqualLine("cpu,tag1=toto,tag2=foo load=42,temperature=53 7");
}


struct Response {
    Result[] results;
}

struct Result {
    MeasurementSeries[] series;
    int statement_id;
}

struct MeasurementSeries {
    import asdf: serializationFlexible;
    string name;
    string[] columns;
    @serializationFlexible string[][] values;

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

    Rows rows() @safe pure nothrow {
        return Rows(columns, values);
    }
}

@("MeasurementSeries.rows")
@safe unittest {

    import std.datetime: SysTime, DateTime, UTC;
    import std.array: array;

    auto series = MeasurementSeries("coolness",
                                    ["time", "foo", "bar"],
                                    [["2015-06-11T20:46:02Z", "red", "blue"]]);

    series.rows[0]["foo"].shouldEqual("red");
    series.rows[0]["time"].shouldEqual("2015-06-11T20:46:02Z");
    series.rows[0].time.shouldEqual(SysTime(DateTime(2015, 06, 11, 20, 46, 2), UTC()));

    series.rows.array.shouldEqual(
        [
            MeasurementSeries.Rows.Row(["time", "foo", "bar"],
                                       ["2015-06-11T20:46:02Z", "red", "blue"]),
        ]
    );
}


version(unittest) {
    /**
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
