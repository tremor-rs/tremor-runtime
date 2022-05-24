
```tremor

# Define a pipeline called `identity`
define pipeline identity
pipeline
  select event from in into out;
end;

# Create an instance of the pipeline
create pipeline identity
````

