
class Constants(object):
    CMD_BYTES_SIZE   = 1 
    KEY_BYTES_SIZE   = 2
    TAGS_BYTES_SIZE  = 4
    VALUE_BYTES_SIZE = 8
    # 
    F64_BYTES_SIZE   = 8
    # __________________
    U8_BYTES_SIZE    = 1
    U16_BYTES_SIZE   = 2
    U32_BYTES_SIZE   = 4
    U64_BYTES_SIZE   = 8
    U128_BYTES_SIZE  = 16
    # 
    PUT                       = 0
    GET                       = 1
    GET_METADATA              = 2
    SET_TAGS                  = 3
    DEL                       = 4
    CHECK_INTEGRITY           = 5
    EXIT                      = 6
    GENERATE_TOKEN            = 7
    REFRESH_TOKEN             = 8
    DESTROY_TOKEN             = 9
    ADD_NODE                  = 10
    BALANCE                   = 11
    LIST_NODES                = 12
    NOT_FOUND                 = 13
    DECODE_ERROR              = 14
    MAX_CLIENT_REACHED        = 15
    UNAUTHORIZED              = 16
    NOT_FOUND_AVAILABLE_NODES = 17
    VERIFIED_TOKEN            = 18
    HEARTBEAT                 = 19
    GRANT_ACCESS              = 20
    BAD_REQUEST               = 21
    ERROR                     = 22
    UNPROCESSABLE             = 23

    # VERIFIED_TOKEN     = 19