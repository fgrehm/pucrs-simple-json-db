1. Create the datafile

```
go run create_datafile.go
```

2. Sample that recreate frames of memory

```
time go run recreate_frames.go

bytes obtained from system:        44MB
bytes allocated and not yet freed: 5MB
bytes in idle spans:               37MB
bytes released to the OS:          0KB

real    1m52.351s
user    0m0.000s
sys     0m0.000s
```

3. Sample that reuses frames of memory

```
time go run reuse_frames.go
bytes obtained from system:        2MB
bytes allocated and not yet freed: 2MB
bytes in idle spans:               0MB
bytes released to the OS:          0KB

real    0m10.354s
user    0m0.000s
sys     0m0.000s
```
