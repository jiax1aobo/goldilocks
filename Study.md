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
>>> Begin:
^                  ^                              ^
write              write                          write
start              protocol                       end
position           position                       position
>>> Begin-Write:   ^                              ^
Cursor             cursor                         cursor
                   current                        end
                   position                       position
>>> End-Write:     ^               ^
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

```c
// 通过网络传输数值型基本数据类型时使用[标志]+[数值]的编码格式
// 取值在 CMP_VAR_INT_1BYTE_MIN ～ CMP_VAR_INT_1BYTE_MAX 范围时直接使用标志字节（一个字节）来传输
// 取值不在上述范围时标志字节的取值可能是 CMP_VAR_INT_2BYTE_FLAG、CMP_VAR_INT_4BYTE_FLAG 或 CMP_VAR_INT_8BYTE_FLAG
//      后面跟着对应标志表示的字节数的数据
#define CMP_VAR_INT_1BYTE_MIN        ( -125 ) /* 0x83 */
#define CMP_VAR_INT_1BYTE_MAX        ( 0x7F )
#define CMP_VAR_INT_2BYTE_FLAG       ( -126 ) /* 0x82 */
#define CMP_VAR_INT_4BYTE_FLAG       ( -127 ) /* 0x81 */
#define CMP_VAR_INT_8BYTE_FLAG       ( -128 ) /* 0x80 */
// 如下是传输 16 位整数的例子：
// 1. 检查写 buffer 能否容纳要写的数据，这里是检查能否能写下 9 个字节
//    实际上根据下面的分支（if 分支写 1 个字节，else 分支写 3 个字节）检查能否再写 3 个字节即可
// 2. 对于范围在 [CMP_VAR_INT_1BYTE_MIN, CMP_VAR_INT_1BYTE_MAX] 的数据直接传输该数据
// 3. 对于不在范围内的数据，先传输 CMP_VAR_INT_2BYTE_FLAG 代表的 2 字节标志
//    然后传输实际的数据，先传输高字节，后传输低字节（大端字节序，符合网络字节序）
#define CMP_WRITE_VAR_INT16( aHandle, aCursor, aValue, aErrorStack )                       \
    {                                                                                      \
        CMP_CHECK_N_WRITABLE( aHandle, aCursor, 9, aErrorStack );                          \
        if( ((stlInt64)*(aValue) >= CMP_VAR_INT_1BYTE_MIN) &&                              \
            ((stlInt64)*(aValue) <= CMP_VAR_INT_1BYTE_MAX) )                               \
        {                                                                                  \
            *((aCursor)->mCurPos) = *(aValue);                                             \
            (aCursor)->mCurPos += 1;                                                       \
        }                                                                                  \
        else                                                                               \
        {                                                                                  \
            *((aCursor)->mCurPos) = CMP_VAR_INT_2BYTE_FLAG;                                \
            *((aCursor)->mCurPos + 1) = (*(aValue) & 0xFF00) >> 8;                         \
            *((aCursor)->mCurPos + 2) = *(aValue) & 0xFF;                                  \
            (aCursor)->mCurPos += 3;                                                       \
        }                                                                                  \
    }
```