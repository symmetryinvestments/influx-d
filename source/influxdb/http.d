/**
    This module implements utility functions for Influx REST API

    Authors: Atila Neves (Kaleidic Associates Advisory Limited)

    Generated documentation:
        http://influxdb.code.kaleidic.io/influxdb.html

*/
module influxdb.http;

import requests : Request, Response, urlEncoded;


///
void manage(in string url, in string str) {
    httpPostRequest(url ~ "/query", ["q": str]);
}

///
string query(in string url, in string db, in string query) {
    return httpGetRequest(url ~ "/query", ["db": db, "q": query]);
}

///
void write(in string url, in string db, in string line) {
    httpPostRequest(url ~ "/write?db=" ~ urlEncoded(db), cast(ubyte[]) line);
}


private string httpGetRequest(in string url,
                              string[string] queryParams,
                              in string file = __FILE__,
                              in size_t line = __LINE__) {
    auto res = Request().get(url, queryParams);
    return processResponse(res, file, line);
}

private string httpPostRequest(in string url,
                               string[string] postParams,
                               in string file = __FILE__,
                               in size_t line = __LINE__) {
    auto res = Request().post(url, postParams);
    return processResponse(res, file, line);
}

private string httpPostRequest(in string url,
                               in ubyte[] data,
                               in string file = __FILE__,
                               in size_t line = __LINE__) {
    auto res = Request().post(url, data, "application/x-www-form-urlencoded");
    return processResponse(res, file, line);
}

private string processResponse(Response response, in string file, in size_t line) {
    const ret = response.responseBody.toString();
    if (response.code < 200 || response.code > 299)
        throw new Exception(ret, file, line);
    return ret;
}
