

~~~tremor
[
  "right bit shift (signed)",
  42 >> 0, 	# 0
  42 >> 2, 	# 10
  -42 >> 2,	# -11
  42 >> 63,	# 0

  # right bit shift (signed) (invalid)
  #42 >> 64,
  #42 >> -1,
  #42 >> 2.0,
  #42 >> "2",
  #42 >> true

  "right bit shift (unsigned)",
  42 >>> 0,	# 42
  42 >>> 2,	# 10
  -42 >>> 2,	# 4611686018427387893
  42 >>> 63,	# 0

  # right bit shift (unsigned) (invalid)
  #42 >>> 64,
  #42 >>> -1,
  #42 >>> 2.0,
  #42 >>> "2",
  #42 >>> true

  "left bit shift",
  42 << 0,	# 42
  42 << 2,	# 168
  -42 << 2,	# -168
  42 << 63	# 0

  # left bit shift (invalid)
  #42 << 64
  #a << 64
  #42 << -1
  #42 << 2.0
  #42 << "2"
  #a << "2"
  #42 << true
  #42 <<< 2
]
~~~
