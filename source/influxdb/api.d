module influxdb.api;

version(unittest) import unit_threaded;


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
