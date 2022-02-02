/++
Conversion utilities that help to work with Mir Series and ndslice.

To use this module `mir-algorithm` package should be included into users `dub.json` file.

Public_imports:
    mir.timeserires

Authors: Ilya Yaroshenko
Copyright: Kaleidic Associates Advisory Limited
License: BSD 3-clause
+/
module influxdb.mir;

////////////////////////////////////
import mir.series;
import influxdb.api;
import std.datetime: DateTime;

/++
Converts MeasurementSeries.Rows to Mir Series.

Params:
    T = Time type. Default time type is DateTime. Supported types are SysTime, DateTime, and Date.
    D = Data type. Default data type is double.
    rows = MeasurementSeries rows
    columns = List of columns (optional). The "time" colummn is ignored.
Returns:
    2D Mir $(LINK2 https://docs.algorithm.dlang.io/latest/mir_series.html, Series).
+/
Series!(T*, D*, 2)
    toMirSeries(T = DateTime, D = double)(
        MeasurementSeries.Rows rows,
        const(string)[] columns = null)
{
    // if columns are not set use all columns
    if (columns is null)
    {
        columns = rows.columns;
    }
    // always exclude "time" column
    foreach (i, column; columns)
    {
        if (column == "time")
        {
            columns = columns[0 .. i] ~ columns[i + 1 .. $];
            break;
        }
    }
    import mir.timestamp: Timestamp;
    import mir.ndslice.allocation: slice, uninitSlice;
    import mir.ndslice.topology: map, as;
    import mir.array.allocation: array;
    import std.conv: to;
    auto time = rows["time"].array.map!(a => a.get!string.Timestamp).as!T.slice;
    auto data = uninitSlice!D(time.length, columns.length);
    foreach (i, column; columns)
    {
        auto from = rows[column];
        foreach (ref elem; data[0 .. $, i])
        {
            elem = from.front.get!D;
            from.popFront;
        }
        assert(from.empty);
    }
    return time.series(data);
}

///
@("toMirSeries")
unittest
{
    import mir.series;
    import std.datetime: DateTime;

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
