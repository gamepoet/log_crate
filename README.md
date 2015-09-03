# LogCrate

An append-only key-value storage (i.e. log-structured storage or a log file) module for Elixir. Appends are made serially but random reads can be made in parallel.

A crate assumes control over a directory in which it maintains a set of files that are segments of the stored records. At configurable time and/or size intervals, old segments are removed to reclaim space. Each record stored in the crate is assigned an integer id which increases linearly as records added.

## Usage

```elixir
crate = LogCrate.create("./my_crate")
# => #PID<0.117.0>

LogCrate.append(crate, "whatever")
# => 0

LogCrate.append(crate, ["a", "bunch", "of", "records"])
LogCrate.read(crate, 2)
# => "bunch"

LogCrate.read(crate, 4)
# => "records"

LogCrate.read(crate, 1, 1024)
# => ["bunch", "of", "records"]

LogCrate.read(crate, 1, 7)
# => ["bunch", "of"]
```
