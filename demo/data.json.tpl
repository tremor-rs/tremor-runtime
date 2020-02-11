// https://next.json-generator.com
[
  {
    'repeat(100)': {
	  about: '{{lorem(1, "paragraphs")}}',
      guid: '{{guid()}}',
      abool: '{{bool()}}',
      aint: '{{integer(20, 40)}}',
      short_message: '{{random("info", "INFO", "ERROR", "error", "warn")}}',
      tags: [
        {
          'repeat(5)': '{{random("tag1", "tag2", "tag3", "tag4", "tag5")}}'
        }
      ],
      array: range(10),
      index_type: '{{random("applog_app3", "applog_app4", "applog_app5", "applog_app6", "syslog_app1", "syslog_app3", "syslog_app4", "syslog_app5", "syslog_app6", "edilog", "sqlserverlog")}}',
      logger_name: '{{random("logger1", "logger2", "logger3")}}',
      syslog_hostname: '{{random("host1", "host2", "host3", "host4", "host5", "host6")}}',
      application: '{{random("app1", "app2", "app3", "app4", "app5", "app6")}}',
      type: '{{random("applog", "syslog", "sillylog")}}',
      'doc-type': 'doc'
    }
  }
]
