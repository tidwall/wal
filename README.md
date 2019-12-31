# `wal`
[![GoDoc](https://godoc.org/github.com/tidwall/wal?status.svg)](https://godoc.org/github.com/tidwall/wal)

Write ahead log for Go.

## Features

- High durability
- Fast writes
- Low memory footprint
- Monotonic indexes
- Log truncation from front or back.

## Getting Started

### Installing

To start using `wal`, install Go and run `go get`:

```sh
$ go get -u github.com/tidwall/wal
```

This will retrieve the library.

### Example

```go
// open a new log file
l, _ := Open("mylog", nil)

// write some entries
l.Write(1, []byte("first entry"))
l.Write(2, []byte("second entry"))
l.Write(3, []byte("third entry"))

// read an entry
data, _ := l.Read(1)
println(string(data))  // output: first entry

// close the log
l.Close()
```

## Contact

Josh Baker [@tidwall](http://twitter.com/tidwall)

## License

`wal` source code is available under the MIT [License](/LICENSE).
