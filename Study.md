# Protocol Sentence Initial Status

## Read Buffer
```
|<------------------ read buffer ---------------->|
|<-- packet hdr -->|
|-------------------------------------------------|
^                  ^
read               read
start              protocol
position           position
                   ^
                   read end
                   position
```

## Write Buffer
```
|<----------------- write buffer ---------------->|
|<-- packet hdr -->|<-- payload -->|
|-------------------------------------------------|
Begin:
^                  ^                              ^
write              write                          write
start              protocol                       end
position           position                       position
Begin-Write:       ^                              ^
Cursor             cursor                         cursor
                   current                        end
                   position                       position
End-Write:         ^               ^
Buffer                             cursor         cursor
                                   curr           end
                                   pos            pos
                                   write          write
                                   protocol       end
                                   position       pos
End-Send:          ^
Buffer             write                          write
                   protocol                       end
                   position                       pos
```