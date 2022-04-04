
### Field selection for records

Selecing a record field using array notation

```tremor
let snot = badger["snot"]
```

Select the field 'snot' from the record 'badger'

### Ordinal selection for arrays

```tremor
let e = badger[0];
```

Select the 0th ( first ) element of the array 'badger'

### Range selection for arrays

```tremor
let e = badger[0:5];
```

Select the 0th up to but no including the 5th element of the array 'badger'


