/**
    This module implements integration tests for Influx API

    Authors: Atila Neves (Kaleidic Associates Advisory Limited)

    Generated documentation:
        http://influxdb.code.kaleidic.io/influxdb.html

*/
module integration.ion;

import influxdb.api;
import unit_threaded;

///
@("deserialise Response")
@system unittest {
    import mir.ion.deser.json: deserializeJson;
    enum jsonString = `
        {
            "results": [{
                    "series": [{
                            "columns": ["time", "othervalue", "tag1", "tag2", "value"],
                            "name": "myname",
                            "values": [
                                    ["2015-06-11T20:46:02Z", 4, "toto", "titi", 2],
                                    ["2017-03-14T23:15:01.06282785Z", 3, "letag", "othertag", 1]
                            ]
                    }],
                    "statement_id": 42
            }]
        }
        `;

    jsonString.deserializeJson!Response.shouldEqual(
        Response(
            [
                Result(
                    [
                        MeasurementSeries(
                            "myname", //name
                            ["time", "othervalue", "tag1", "tag2", "value"], //columns
                            //values
                            [
                                ["2015-06-11T20:46:02Z".InfluxValue, 4.InfluxValue, "toto".InfluxValue, "titi".InfluxValue, 2.InfluxValue],
                                ["2017-03-14T23:15:01.06282785Z".InfluxValue, 3.InfluxValue, "letag".InfluxValue, "othertag".InfluxValue, 1.InfluxValue],
                            ]
                        ),
                    ],

                    42, // statement_id
                )
            ]
        )
    );
}
