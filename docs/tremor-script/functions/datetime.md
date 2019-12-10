# The `datetime` Namespace

The time namespace contains functions that provide easier retrieval, manipulation and conversion to or from timestamps.

* All datetime values are of type `long`
* Timestamps have nanosecond precision
* All timestamps are considered as UTC

## Functions

### datetime::parse(datetime, input_format)

Creates a new timestamp from the datetime string passed to the function and the format of the input string being passed. Fails if an incorrect string is passed to the function. The input formats supported are mentioned below.

The function errors if:

* Incorrect input is passed
* Input doesn't match the format passed
* Input doesn't contain the Year, Month, Day, Hour & Minute section irrespective of the format passed.
* Input contains more components than the format passed

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

Full ![reference](https://www.iso.org/obp/ui#iso:std:iso:8601:-1:ed-1:v1:en)

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

```tremor
datetime::month(1559655782000000000)
## output: 6
```

### datetime::day(datetime)

Returns the day of the month from a given timestamp.

```tremor
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

```tremor
datetime::minute(1559655782000000000)
## output: 43
```

### datetime::second(datetime)

Returns the second from a given timestamp.

```tremor
datetime::second(1559655782000000000)
## output: 2
```

### datetime::millisecond(datetime) / datetime::microsecond(datetime) / datetime::nanosecond(datetime)

Returns the corresponding millisecond, microsecond or nanosecond component of the timestamp.

```tremor
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

Returns the timestamp for the beginning of the current day.

````tremor
datetime::today()
## output: 156072960000000000
````

### datetime::with_nanoseconds(n)

Returns a timestamp representing an interval of `n` nanoseconds

### datetime::with_milliseconds(n)

Returns a timestamp representing an interval of `n` milliseconds

### datetime::with_hours(n)

Returns a timestamp representing an interval of `n` hours.

### datetime::with_days(n)

Returns a timestamp representing an interval of `n` days.

### datetime::with_minutes(n)

Returns a timestamp representing interval of `n` minutes.

### datetime::with_seconds(n)

Returns a timestamp representing an interval of `n` seconds

### datetime::with_weeks(n)

Returns a timestamp representing an interval of `n` weeks

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

Returns the timestamp subseconds component.

```tremor
datetime::without_subseconds()
```

## Input Formats

Input formats can contain the following tokens:

The following specifiers are available both to formatting and parsing.

| Spec.  | Example                            | Description                                                  |
| :----- | :--------------------------------- | :----------------------------------------------------------- |
|        |                                    | **DATE SPECIFIERS:**                                         |
| `%Y`   | `2001`                             | The full proleptic Gregorian year, zero-padded to 4 digits. [1] |
| `%C`   | `20`                               | The proleptic Gregorian year divided by 100, zero-padded to 2 digits. [2] |
| `%y`   | `01`                               | The proleptic Gregorian year modulo 100, zero-padded to 2 digits. [2] |
|        |                                    |                                                              |
| `%m`   | `07`                               | Month number (01--12), zero-padded to 2 digits.              |
| `%b`   | `Jul`                              | Abbreviated month name. Always 3 letters.                    |
| `%B`   | `July`                             | Full month name. Also accepts corresponding abbreviation in parsing. |
| `%h`   | `Jul`                              | Same to `%b`.                                                |
|        |                                    |                                                              |
| `%d`   | `08`                               | Day number (01--31), zero-padded to 2 digits.                |
| `%e`   | `8`                                | Same to `%d` but space-padded. Same to `%_d`.                |
|        |                                    |                                                              |
| `%a`   | `Sun`                              | Abbreviated weekday name. Always 3 letters.                  |
| `%A`   | `Sunday`                           | Full weekday name. Also accepts corresponding abbreviation in parsing. |
| `%w`   | `0`                                | Sunday = 0, Monday = 1, ..., Saturday = 6.                   |
| `%u`   | `7`                                | Monday = 1, Tuesday = 2, ..., Sunday = 7. (ISO 8601)         |
|        |                                    |                                                              |
| `%U`   | `28`                               | Week number starting with Sunday (00--53), zero-padded to 2 digits. [3] |
| `%W`   | `27`                               | Same to `%U`, but week 1 starts with the first Monday in that year instead. |
|        |                                    |                                                              |
| `%G`   | `2001`                             | Same to `%Y` but uses the year number in ISO 8601 week date. [4] |
| `%g`   | `01`                               | Same to `%y` but uses the year number in ISO 8601 week date. [4] |
| `%V`   | `27`                               | Same to `%U` but uses the week number in ISO 8601 week date (01--53). [4] |
|        |                                    |                                                              |
| `%j`   | `189`                              | Day of the year (001--366), zero-padded to 3 digits.         |
|        |                                    |                                                              |
| `%D`   | `07/08/01`                         | Month-day-year format. Same to `%m/%d/%y`.                   |
| `%x`   | `07/08/01`                         | Same to `%D`.                                                |
| `%F`   | `2001-07-08`                       | Year-month-day format (ISO 8601). Same to `%Y-%m-%d`.        |
| `%v`   | `8-Jul-2001`                       | Day-month-year format. Same to `%e-%b-%Y`.                   |
|        |                                    |                                                              |
|        |                                    | **TIME SPECIFIERS:**                                         |
| `%H`   | `00`                               | Hour number (00--23), zero-padded to 2 digits.               |
| `%k`   | `0`                                | Same to `%H` but space-padded. Same to `%_H`.                |
| `%I`   | `12`                               | Hour number in 12-hour clocks (01--12), zero-padded to 2 digits. |
| `%l`   | `12`                               | Same to `%I` but space-padded. Same to `%_I`.                |
|        |                                    |                                                              |
| `%P`   | `am`                               | `am` or `pm` in 12-hour clocks.                              |
| `%p`   | `AM`                               | `AM` or `PM` in 12-hour clocks.                              |
|        |                                    |                                                              |
| `%M`   | `34`                               | Minute number (00--59), zero-padded to 2 digits.             |
| `%S`   | `60`                               | Second number (00--60), zero-padded to 2 digits.             |
| `%f`   | `026490000`                        | The fractional seconds (in nanoseconds) since last whole second. |
| `%.f`  | `.026490`                          | Similar to `.%f` but left-aligned. These all consume the leading dot. |
| `%.3f` | `.026`                             | Similar to `.%f` but left-aligned but fixed to a length of 3. [8] |
| `%.6f` | `.026490`                          | Similar to `.%f` but left-aligned but fixed to a length of 6. [8] |
| `%.9f` | `.026490000`                       | Similar to `.%f` but left-aligned but fixed to a length of 9. [8] |
| `%3f`  | `026`                              | Similar to `%.3f` but without the leading dot.               |
| `%6f`  | `026490`                           | Similar to `%.6f` but without the leading dot.               |
| `%9f`  | `026490000`                        | Similar to `%.9f` but without the leading dot.               |
|        |                                    |                                                              |
| `%R`   | `00:34`                            | Hour-minute format. Same to `%H:%M`.                         |
| `%T`   | `00:34:60`                         | Hour-minute-second format. Same to `%H:%M:%S`.               |
| `%X`   | `00:34:60`                         | Same to `%T`.                                                |
| `%r`   | `12:34:60 AM`                      | Hour-minute-second format in 12-hour clocks. Same to `%I:%M:%S %p`. |
|        |                                    |                                                              |
|        |                                    | **TIME ZONE SPECIFIERS:**                                    |
| `%Z`   | `ACST`                             | *Formatting only:* Local time zone name.                     |
| `%z`   | `+0930`                            | Offset from the local time to UTC (with UTC being `+0000`).  |
| `%:z`  | `+09:30`                           | Same to `%z` but with a colon.                               |
| `%#z`  | `+09`                              | *Parsing only:* Same to `%z` but allows minutes to be missing or present. |
|        |                                    |                                                              |
|        |                                    | **DATE & TIME SPECIFIERS:**                                  |
| `%c`   | `Sun Jul 8 00:34:60 2001`          | `ctime` date & time format. Same to `%a %b %e %T %Y` sans `\n`. |
| `%+`   | `2001-07-08T00:34:60.026490+09:30` | ISO 8601 / RFC 3339 date & time format. [                    |
|        |                                    |                                                              |
| `%s`   | `994518299`                        | UNIX timestamp, the number of seconds since 1970-01-01 00:00 UTC. [7] |
|        |                                    |                                                              |
|        |                                    | **SPECIAL SPECIFIERS:**                                      |
| `%t`   |                                    | Literal tab (`\t`).                                          |
| `%n`   |                                    | Literal newline (`\n`).                                      |
| `%%`   |                                    | Literal percent sign.                                        |

It is possible to override the default padding behavior of numeric specifiers `%?`. This is not allowed for other specifiers and will result in the `BAD_FORMAT` error.

| Modifier | Description                                                  |
| :------- | :----------------------------------------------------------- |
| `%-?`    | Suppresses any padding including spaces and zeroes. (e.g. `%j` = `012`, `%-j` = `12`) |
| `%_?`    | Uses spaces as a padding. (e.g. `%j` = `012`, `%_j` = `12`)  |
| `%0?`    | Uses zeroes as a padding. (e.g. `%e` = `9`, `%0e` = `09`)    |
