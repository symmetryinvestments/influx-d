module integration.api;

import unit_threaded;
import influxdb;
import integration.common: influxURL;


@Serial
@("Database api")
unittest {

    import influxdb.api: Database, Measurement;

    const database = Database(influxURL, "myspecialDB");
    scope(exit) database.drop;

    database.insert(Measurement("cpu", ["tag1": "foo"], ["temperature": "42"]));
    database.insert(Measurement("cpu", ["tag1": "foo"], ["temperature": "68"]));

    {
        const response = database.query("SELECT * from cpu");
        const result = response.results[0];
        const series = result.series[0];
        series.rows.length.shouldEqual(2);
    }

    {
        const response = database.query("SELECT * from cpu WHERE temperature > 50");
        const result = response.results[0];
        const series = result.series[0];
        series.rows.length.shouldEqual(1);
    }
}
