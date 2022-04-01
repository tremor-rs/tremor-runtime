
Multiple consecutive documentation comments are coalesced into a single comment.

```tremor
## I am a single
## documentation level comment
## split across multiple lines
```

Documentation comments are used by the documentation tool to generate references
such as the standard library on the tremor website.

The following incantation will generate a `docs` folder with markdown generated
for the user defined `lib` library of reusable logic

```bash
$ TREMOR_PATH=/path/to/stdlib:/path/to/otherlib tremor doc /path/to/my/lib
```

