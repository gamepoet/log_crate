# LogCrate

An append-only key-value storage (i.e. log-structured storage or a log file) module for Elixir. Appends are made serially but random reads can be made in parallel.

A crate assumes control over a directory in which it maintains a set of files that are segments of the stored records. At configurable time and/or size intervals, old segments are removed to reclaim space. Each record stored in the crate is assigned an integer id which increases linearly as records added.

## Usage

```elixir
crate = LogCrate.create("./my_crate")
# => #PID<0.117.0>

LogCrate.append(crate, {:crypto.hash(:sha, "whatever"), whatever"})
# => 0

LogCrate.append(crate, ["a", "bunch", "of", "records"] |> Enum.map(fn(val) -> {:crypto.hash(:sha, val), val} end))
LogCrate.read(crate, 2)
# => {<<39, 123, 205, 77, 88, 130, 66, 251, 80, 220, 235, 3, 228, 137, 239, 95, 198, 149, 160, 249>>,
 "bunch"}

LogCrate.read(crate, 4)
# => {<<134, 118, 27, 99, 167, 189, 31, 72, 14, 212, 129, 204, 123, 66, 85, 173, 197, 217, 6, 61>>,
 "records"}

LogCrate.read(crate, 1, 1024)
# => [{<<39, 123, 205, 77, 88, 130, 66, 251, 80, 220, 235, 3, 228, 137, 239, 95, 198, 149, 160, 249>>,
  "bunch"},
 {<<222, 4, 250, 14, 41, 249, 179, 94, 36, 144, 93, 46, 81, 43, 237, 201, 187, 110, 9, 228>>,
  "of"},
 {<<134, 118, 27, 99, 167, 189, 31, 72, 14, 212, 129, 204, 123, 66, 85, 173, 197, 217, 6, 61>>,
  "records"}]

LogCrate.read(crate, 1, 7)
# => [{<<39, 123, 205, 77, 88, 130, 66, 251, 80, 220, 235, 3, 228, 137, 239, 95, 198, 149, 160, 249>>,
  "bunch"},
 {<<222, 4, 250, 14, 41, 249, 179, 94, 36, 144, 93, 46, 81, 43, 237, 201, 187, 110, 9, 228>>,
  "of"}
```
