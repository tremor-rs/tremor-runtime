# Transform

This example shows how handling apache logs with a tremor and elastic search could work. The example is a lot more complex than the simple showcases and combines three components.

Kibana, which once started with docker-compose can be reached [locally](http://localhost:5601). It allows browsing through the logs. If you have never used Kibana before you can get started by clicking on **Management** then in the **Elasticsearch** section on **Index Management**.

Elastic Search, which stores the logs submitted.

Tremor, which takes the apache logs, parses and classifies them then submits them to indexes in elastic search.

In addition the file `demo/data/apache_access_logs.xz` is used as example payload.

## Environment

In the [`example.trickle`](etc/tremor/config/example.trickle) we define a script that validates the schema that the field `hello` is a string and the field `selected` is a boolean. If both conditions are true we pass it on, otherwise, it'll drop.

All other configuration is the same as per the previous example and is elided here for brevity.

## Business Logic

```trickle
define script extract                                                          # define the script that parses our apache logs
script
  match {"raw": event} of                                                      # we user the dissect extractor to parse the apache log
    case r = %{ raw ~= dissect|%{ip} %{} %{} [%{timestamp}] "%{method} %{path} %{proto}" %{code:int} %{cost:int}\\n| }
            => r.raw                                                           # this first case is hit of the log includes an execution time (cost) for the request
    case r = %{ raw ~= dissect|%{ip} %{} %{} [%{timestamp}] "%{method} %{path} %{proto}" %{code:int} %{}\\n| }
            => r.raw         
    default => emit => "bad"
  end
end;

```

```trickle
define script categorize                                                       # defome the script that classifies the logs
with
  user_error_index = "errors",                                                 # we use with here to default some configuration for
  server_error_index = "errors",                                               # the script, we could then re-use this script in multiple
  ok_index = "requests",                                                       # places with different indexes
  other_index = "requests"
script
  let $doc_type = "log";                                                      # doc_type is used by the offramp, the $ denots this is stored in event metadat
  let $index = match event of
    case e = %{present code} when e.code >= 200 and e.code < 400              # for http codes between 200 and 400 (exclusive) - those are success codes
      => args.ok_index
    case e = %{present code} when e.code >= 400 and e.code < 500              # 400 to 500 (exclusive) are client side errors
      => args.user_error_index
    case e = %{present code} when e.code >= 500 and e.code < 600
      => args.server_error_index                                              # 500 to 500 (exclusive) are server side errors
    default => args.other_index                                               # if we get any other code we just use a default index
  end;
  event                                                                       # emit the event with it's new metadata
end;
```

## Command line testing during logic development

```bash
$ docker-compose up
  ... lots of logs ...
```

Inject test messages via [websocat](https://github.com/vi/websocat)

> Note: Can be installed via `cargo install websocat` for the lazy/impatient amongst us

```bash
$ cat demo/data/apache_access_logs.xz | websocat ws://localhost:4242
...
```

Open the [Kibana index management](http://localhost:5601/app/kibana#/management/kibana/indices/) and create indexes to view the data.

### Discussion

This is a fairly complex example that combines everything we've seen in the prior examples and a bit more. It should serve as a starting point of how to use tremor to ingest, process, filter and classify data with tremor into an upstream system.

### Attention

When using this as a baseline be aware that around things like batching tuning will be involved to make the numbers fit with the infrastructure it is pointed at. Also since it is not an ongoing data stream we omitted backpressure or classification based rate limiting from the example.