/++
Include mir-algorithm into your project.

See_also:
	$(LINK2 http://docs.algorithm.dlang.io/latest/mir_timeseries.html, mir.timeseries).
+/
import influxdb;
import mir.timeseries;
import std.datetime: DateTime;

void main()
{
	auto influxSeries = MeasurementSeries("coolness",
        ["time", "foo", "bar"],
        [
            ["2015-06-11T20:46:02Z", "1.0", "2.0"],
            ["2013-02-09T12:34:56Z", "3.0", "4.0"],
        ]);

    auto series = influxSeries.rows.toMirSeries;

    // sort data if required
    {
        import mir.ndslice.algorithm: all;
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
