## Datetime Namespace 

The time namespace contains functions that provide easier retrieval, manipulation and conversion to or from timestamps. 

* All datetime values are of type `long`
* Timestamps have nanosecond precision
* All timestamps are considered as UTC

  

### datetime::parse(datetime, input_format) 

Creates a new timestamp from the datetime string passed to the function and the format of the input string being passed. Fails if an incorrect string is passed to the function. 

```tremor
datetime::parse("1983 Apr 13 12:09:14.274 +0000", "%Y %b %d %H:%M:%S%.3f %z") 
## output: 419083754274000000                   
```



### datetime::format(datetime, format_string)

Converts a timestamp to the format specified in the format_string. Accepts format strings which contain the respective components of the format string (%Y, %m, %d, %H, %M, %S  )

```tremor
datetime::format(event.syslog_timestamp, "%Y-%m-%d %H:%M:%S%.9f")
## output: 2019-04-20 9:41:22.123456789
```



### datetime::iso8601(datetime)

Converts a timestamp to the format conforming to the iso8601 specification. 

* reference: <https://www.iso.org/obp/ui#iso:std:iso:8601:-1:ed-1:v1:en>

```tremor
datetime::iso8601(event.syslog_timestamp)
## output: 2019-06-04T13:43:02.123456789+00:00
```

### datetime::year(datetime)

Returns the year from a given timestamp. 

```tremor
datetime::year(1559655782000000000)
## output: 2019
```

### datetime::month(datetime)

Returns the month from a given timestamp in a numerical format

```
datetime::month(1559655782000000000)
## output: 6
```

### datetime::day(datetime)

Returns the day of the month from a given timestamp. 

```
datetime::day(1559655782000000000)
## output: 17
```

### datetime::hour(datetime)

Returns the hour from a given timestamp. 

```tremor
datetime::hour(1559655782000000000)
## output: 13
```

### datetime::minute(datetime)

Returns the minute from a given timestamp. 

```
datetime::minute(1559655782000000000)
## output: 43
```

### datetime::second(datetime)

Returns the second from a given timestamp. 

```
datetime::second(1559655782000000000)
## output: 2
```

### datetime::millisecond(datetime) / datetime::microsecond(datetime) / datetime::nanosecond(datetime)

Returns the corresponding millisecond, microsecond or nanosecond component of the timestamp. 

```
datetime::millisecond(1559655782123456789)
## output: 123
datetime::microsecond(1559655782123456789)
## output: 456
datetime::nanosecond(1559655782123456789)
## output: 789
```



### datetime::to_nearest_millisecond(datetime) / datetime::to_nearest_microsecond(datetime) / datetime::to_nearest_second()

Rounds the given time to the nearest micro, milli or second. If the fractional part is above half of the nearest component (e.g. millisecond for nearest_millisecond), it will be rounded to the value of the component, else it will be truncated.

```tremor
datetime::to_nearest_millisecond(1559655782123456789)
## output: 1559655782123000000
datetime::to_nearest_microsecond(1559655782123456789)
## output: 1559655782123457000
datetime::to_nearest_nsecond(1559655782123456789)
## output: 1558655782000000000
```



### datetime::from_human_format(human_format_string) 

Returns a timestamp representing the interval specified in human format (e.g. "5 minutes", "10 hours"). Fails if an incorrect format is given. 

```tremor
datetime::from_human_format("1 minute")
## output: 60000000000
datetime::from_human_format("10 seconds")
## output: 10000000000
datetime::from_human_format("1 day 20 minutes 5 seconds 136 milliseconds")
## output: 8760500013600000000
datetime::from_human_format("17 years 2 weeks 1 day")
## output: 5377536000000000000
```

### datetime::today()

Returns the timestamp for the beginning of the current day 

````tremor
datetime::today()
## output: 156072960000000000
````

### datetime::with_nanoseconds(n)

Returns a timestamp representing an interval of _n_ nanoseconds

### datetime::with_milliseconds(n)

Returns a timestamp representing an interval of _n_ milliseconds

### datetime::with_hours(n) 

Returns a timestamp representing an interval of _n_ hours 

### datetime::with_days(n)

Returns a timestamp representing an interval of _n_ days

### datetime::with_minutes(n)

Returns a timestamp representing interval of _n_ minutes

### datetime::with_seconds(n)

Returns a timestamp representing an interval of _n_ seconds

### datetime::with_weeks(n)

Returns a timestamp representing an interval of _n_ weeks

```tremor
datetime::with_naoseconds(134)
### output: 134
datetime::with_microseconds(269)
### output: 269000
datetime::with_milliseconds(169)
### output: 169000000
datetime::with_seconds(13)
## output: 130000000000
datetime::with_minutes(1)
## output: 60000000000
datetime::with_days(12)
## output: 1382400000
datetime::with_weeks(2)
## output: 1123200000000000
datetime::with_years(10)
315532800000000000
```



### datetime::without_subseconds() 

Returns the timestamp  subseconds component. 

```tremor
datetime::without_subseconds()
```



### 

