package influxdb

import "influxdata/influxdb"
import "influxdata/influxdb/v1"

// _mask will hide the given columns from downstream
// transformations. It will not perform any copies and
// it will not regroup. This should only be used when
// the user knows it can't cause a key conflict.
builtin _mask

// select will select data from an influxdb instance within
// the range between `start` and `stop` from the bucket specified by
// the `from` parameter. It will select the specific measurement
// and it will only include fields that are included in the list of
// `fields`.
//
// In order to filter by tags, the `where` function can be used to further
// limit the amount of data selected.
select = (from, start, stop=now(), m, fields, org="", host="", token="", where=(r) => true) => {
    source =
        if org != "" and host != "" and token != "" then
            influxdb.from(bucket: from, org: org, host: host, token: token)
        else if org != "" and token != "" then
            influxdb.from(bucket: from, org: org, token: token)
        else if org != "" and host != "" then
            influxdb.from(bucket: from, org: org, host: host)
        else if host != "" and token != "" then
            influxdb.from(bucket: from, host: host, token: token)
        else if org != "" then
            influxdb.from(bucket: from, org: org)
        else if host != "" then
            influxdb.from(bucket: from, host: host)
        else if token != "" then
            influxdb.from(bucket: from, token: token)
        else
            influxdb.from(bucket: from)

    tables = source
        |> range(start, stop)
        |> filter(fn: (r) => r._measurement == m)
        |> filter(fn: where)

    nfields = length(arr: fields)
    filtered = if nfields == 1 then
            tables |> filter(fn: (r) => r._field == fields[0])
        else if nfields == 2 then
            tables |> filter(fn: (r) => r._field == fields[0] or r._field == fields[1])
        else if nfields == 3 then
            tables |> filter(fn: (r) => r._field == fields[0] or r._field == fields[1] or r._field == fields[2])
        else if nfields == 4 then
            tables |> filter(fn: (r) => r._field == fields[0] or r._field == fields[1] or r._field == fields[2] or r._field == fields[3])
        else if nfields == 5 then
            tables |> filter(fn: (r) => r._field == fields[0] or r._field == fields[1] or r._field == fields[2] or r._field == fields[3] or r._field == fields[4])
        else
            tables |> filter(fn: (r) => contains(value: r._field, set: fields))
    return filtered |> v1.fieldsAsCols() |> _mask(columns: ["_measurement", "_start", "_stop"])
}
