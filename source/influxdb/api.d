module influxdb.api;

version(unittest) import unit_threaded;


struct Measurement {

    string name;
    string[string] tags;
    string[string] fields;
    long timestamp;

    @disable this();

    this(string name, string[string] tags, string[string] fields) @safe pure {
        this.name = name;
        this.tags = tags;
        this.fields = fields;
        this.timestamp = 1434055562000000000;
    }

    string toString() @safe pure const {
        import std.range: chain;
        import std.conv: text;
        import std.algorithm: joiner;

        // @trusted due to aa.keys
        auto aaToString(in string[string] aa) @trusted {
            import std.algorithm: map;
            return aa.keys.map!(k => k ~ "=" ~ aa[k]);
        }

        return text(chain([name], aaToString(tags)).joiner(","),
                    " ", aaToString(fields).joiner(","),
                    " ", timestamp,
        );
    }
}


@("Measurement.toString no timestamp")
@safe pure unittest {
    {
        auto m = Measurement("cpu",
                             ["tag1": "toto", "tag2": "foo"],
                             ["load": "42", "temperature": "53"]);
        m.toString.shouldEqualLine("cpu,tag1=toto,tag2=foo load=42,temperature=53 1434055562000000000");
    }

    {
        auto m = Measurement("thingie",
                             ["foo": "bar"],
                             ["value": "7"]);
        m.toString.shouldEqualLine("thingie,foo=bar value=7 1434055562000000000");
    }
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
            assert(parts.length == 3,
                   text("Illegal number of parts: ", parts.length));

            auto nameTags = parts[0].split(",");
            const name = nameTags[0];
            auto tags = nameTags[1..$];

            auto fields = parts[1].split(",");

            return chain([name], sort(tags)).join(",") ~ " " ~
                sort(fields).join(",") ~ " " ~
                parts[2];
        }

        sortLine(actual).shouldEqual(sortLine(expected), file, line);
    }
}
