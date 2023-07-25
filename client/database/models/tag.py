import json
from datetime import datetime, timezone

class Tag():
    next_logging : float = 0
    next_batching = 0
    
    def __init__(self, id, dbNr, startAddr, dataType, logInterval, batchInterval, PLC_IP):
        self._id = id
        self.db_nr = dbNr
        self.start_address = startAddr
        self.data_type = dataType
        self.log_interval = logInterval
        self.batch_interval = batchInterval
        self.PLC_IP = PLC_IP
        
# class LoggedTag():
#     def __init__(self, id, value, PLC_IP) -> None:
#         self.id = id
#         self.value = value
#         self.PLC_IP = PLC_IP
#         self.timestamp = datetime.now(timezone.utc).isoformat()