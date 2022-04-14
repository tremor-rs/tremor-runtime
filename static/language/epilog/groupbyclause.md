
```tremor
select event from in[one_second]
having present event.important
group by event.priority
into out
```

The `group by` clause groups events based on a group expression. Each computed group effectively
has its own memory and computation allocated.

For windowed operations the windows are allocated for each group.

These groups and their windows are independant. This means that opening, closing, filling and
recycling of windows is by group.

