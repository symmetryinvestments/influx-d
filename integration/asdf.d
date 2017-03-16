module integration.asdf;

import asdf;
import influxdb.api;
import unit_threaded;


@("deserialise Response")
@system unittest {
    enum jsonString = `
        {
            "results": [{
                    "series": [{
                            "columns": ["time", "othervalue", "tag1", "tag2", "value"],
                            "name": "myname",
                            "values": [
                                    ["2015-06-11T20:46:02Z", "4", "toto", "titi", "2"],
                                    ["2017-03-14T23:15:01.06282785Z", "3", "letag", "othertag", "1"]
                            ]
                    }],
                    "statement_id": 42
            }]
        }
        `;

    jsonString.deserialize!Response.shouldEqual(
        Response(
            [
                Result(
                    [
                        Table(
                            ["time", "othervalue", "tag1", "tag2", "value"], //columns
                            "myname", //name
                            //values
                            [
                                ["2015-06-11T20:46:02Z", "4", "toto", "titi", "2"],
                                ["2017-03-14T23:15:01.06282785Z", "3", "letag", "othertag", "1"],
                            ]
                        ),
                    ],

                    42, // statement_id
                )
            ]
        )
    );
}


void shouldBeSameJsonAs(in string actual,
                        in string expected,
                        string file = __FILE__,
                        size_t line = __LINE__)
    @safe
{
    import std.json;
    actual.parseJSON.toPrettyString.shouldEqual(expected.parseJSON.toPrettyString, file, line);
}
