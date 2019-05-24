## Datetime Namespace 

The time namespace contains functions that provide easier retrieval, manipulation and conversion to or from timestamps. 

* All datetime values are of type `long`
* Timestamps have nanosecond precision
* All timestamps are considered as UTC

  

### Datetime::to_ns(datetime) 

Returns the value as an integer number of seconds since UNIX Epoch (Jan 1st 1970, 00:00 UTC).

### Datetime::parse(datetime) 

Parses a string to a structured representation. 

* default format
* 


### Datetime::utc_offset(datetime)

Returns the offset in seconds between the timezone of the input and UTC

### Datetime::format(time, format)

Converts the time from one format to the format specified e.g. from dd-mm-yy to yy-mm-dd

Alternatives:

* String::format can be used for this 

### Datetime::iso8601(datetime)

Parses to a structural representation that conforms to ISO 8601 format from a string.

* reference: <https://www.iso.org/obp/ui#iso:std:iso:8601:-1:ed-1:v1:en>

### Datetime::hour(datetime)

Returns the hour from a given timestamp. 

### Datetime::to_nearest_millisecond(datetime) / Datetime::to_nearest_microsecond(datetime) / Datetime::to_nearest_second()

Rounds the given time to the nearest micro, milli or second. 

Concerns:

* Requires guarantee that data is precise to a nanosecond
* No usecase right now for the metrics team, but it would help them in the short term. 

### Datetime::from_human_format(human_format_string) 

Returns a timestamp representing the interval specified in human format (e.g. "5 minutes", "10 hours")



---------------------------------------------------------------------------------------------------------------

### Datetime::in_localtime(year, month, day, hour, minute, second) 

Creates a timestamp in the local timezone from the integer components. 

Concerns: 

* accept milliseconds and nanoseconds as parameters or seconds as a float?
* any timeformat other than UTC considered harmful, likely not implementing any local functions

### Datetime::today()

Returns the timestamp for the beginning of the current day in UTC. 

### Datetime::nanoseconds(n)

Returns a timestamp representing an interval of _n_ nanoseconds

### Datetime::milliseconds(n)

Returns a timestamp representing an interval of _n_ milliseconds

### Datetime::hours(n) 

Returns a timestamp representing an interval of _n_ hours 

### Datetime::days(n)

Returns a timestamp representing an interval of _n_ days

### Datetime::minutes(n)

Returns a timestamp representing interval of _n_ minutes

### Datetime::seconds(n)

Returns a timestamp representing an interval of _n_ seconds

### Datetime::weeks(n)

Returns a timestamp representing an interval of _n_ weeks

### Datetime::get_*() 

Returns the portion of the timestamp based on the function called:

* nanosecond()
* millisecond()
* second()
* minute()
* hour()
* day()
* month()
* year()

### Datetime::without_subseconds() 

Returns the timestamp only with subseconds component. 



### 

