# FEATURE COMPARISON

This section compares the latest stable version of Tremor with other alternative products.

| Feature                        |    Tremor    |  Hindsight   |       Logstash        |
| ------------------------------ | :----------: | :----------: | :-------------------: |
| Pipeline Architecture          |      Y       |      Y       |           Y           |
| Additional Runtime             | Not required | Not required | Requires Java runtime |
| Configurable via a script file |      Y       |      Y       |           Y           |
| Open Source                    |      N       |      Y       |           Y           |
| Grok support                   |      Y       |      N       |           Y           |
| JSON Codec                     |      Y       |      Y       |           Y           |
| Windows support                |      N       |      Y       |           Y           |
|                                |              |              |                       |

## Logstash

Logstash is an open source data collection engine with real time pipelining capabilities written by Elasticsearch.  We compare Tremor with the Logstash 7.0 which is the latest stable version at the time of writing.

## Hindsight

Hindsight is a C based data processing infrastructure based on the Lua sandbox.
