/++
Mir-Influx integration example.

Include mir-algorithm into your project.

See_also:
	$(LINK2 http://docs.algorithm.dlang.io/latest/mir_series.html, mir.series).

Authors: Ilya Yaroshenko
+/
import influxdb;
import mir.series;
import std.datetime: Date, DateTime;

void main()
{
    readFromInflux();
    writeToInflux();
}

void writeToInflux()
{
    import mir.string_map: StringMap;
    string[string][] manages;
    string[string][] queries;
    string[string][] writes;

    alias TestDatabase = DatabaseImpl!(
        (url, cmd) => manages ~= ["url": url, "cmd": cmd], // manage
        (url, db, query) { // query
            queries ~= ["url": url, "db": db, "query": query];
            return ``;
        },
        (url, db, line) => writes ~= ["url": url, "db": db, "line": line]
    );

    const database = TestDatabase("http://db.com", "testdb");

    auto time = [
        Date(2017, 2, 1),
        Date(2017, 2, 3),
        Date(2017, 2, 4)].sliced;
    auto data = [
        2, 3,
        -3, 6,
        4, 0].sliced(3, 2); // 3 x 2

    auto series1D = time.series(data.front!1);
    auto series2D = time.series(data);

    database.insert("coins-Alice", "Alice", series1D, ["tag":"1"].StringMap!string);
    database.insert("coins", ["Alice", "Bob"], series2D, ["tag":"2"].StringMap!string);

    assert(writes == [

        ["url": "http://db.com", "db": "testdb", "line": 
        "coins-Alice,tag=1 Alice=2i 1485907200000000000\n" ~
        "coins-Alice,tag=1 Alice=-3i 1486080000000000000\n" ~
        "coins-Alice,tag=1 Alice=4i 1486166400000000000"],

        ["url": "http://db.com", "db": "testdb", "line": 
        "coins,tag=2 Alice=2i,Bob=3i 1485907200000000000\n" ~
        "coins,tag=2 Alice=-3i,Bob=6i 1486080000000000000\n" ~
        "coins,tag=2 Alice=4i,Bob=0i 1486166400000000000"],
    ]);
}

void readFromInflux()
{
	auto influxSeries = MeasurementSeries("coolness",
        ["time", "foo", "bar"],
        [
            ["2015-06-11T20:46:02Z".InfluxValue, 1.0.InfluxValue, 2.0.InfluxValue],
            ["2013-02-09T12:34:56Z".InfluxValue, 3.0.InfluxValue, 4.0.InfluxValue],
        ]);

    auto series = influxSeries.rows.toMirSeries;

    // sort data if required
    {
        import mir.algorithm.iteration: all;
        import mir.ndslice.allocation: uninitSlice;
        import mir.ndslice.topology: pairwise;

        if (!series.time.pairwise!"a <= b".all)
        {
            series.sort(
                uninitSlice!size_t(series.length), // index buffer
                uninitSlice!double(series.length)); // data buffer
        }
    }

    assert(series.time == [
        DateTime(2013,  2,  9, 12, 34, 56),
        DateTime(2015,  6, 11, 20, 46,  2)]);

    assert(series.data == [
        [3.0, 4.0],
        [1.0, 2.0]]);
}
