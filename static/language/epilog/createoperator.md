
```tremor

# Define a round robin operator with 3 slots
define operator roundrobin from qos::roundrobin
with
  outputs = ["one", "two", "three"]
end;

# create an instance of the operator
create operator roundrobin;

# Filter all inbound events into the rond robin
select event from in into roundrobin;

# Union slots inot outbound port
select event from roundrobin/one into out;
select event from roundrobin/two into out;
select event from roundrobin/three into out;
```

