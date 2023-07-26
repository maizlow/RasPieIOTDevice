from enum import IntEnum        

class DataType(IntEnum):
    Bit     = 1
    Byte    = 2
    Char    = 3
    Word    = 4
    Int     = 5
    DWord   = 6
    DInt    = 7
    Real    = 8
    String  = 9

class Tag():
    next_logging : float = 0
    next_batching = 0
    db_nr : int
    start_address : int
    data_type : int
    bit_nr : int
    log_interval : int
    batch_interval : int
    PLC_IP : str
    
    def __init__(self, id, dbNr, startAddr, dataType, bit_nr, logInterval, batchInterval, PLC_IP):
        self._id = id
        self.db_nr = dbNr
        self.start_address = startAddr
        self.data_type = dataType
        self.bit_nr = bit_nr
        self.log_interval = logInterval
        self.batch_interval = batchInterval
        self.PLC_IP = PLC_IP