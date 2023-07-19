class PLC_Model(object):
    _id = int()
    _ip_address = str()
    _rack = int()
    _slot = int()
    _active = bool() #Active means it will be used

    def __init__(self, ip_address, rack, slot, active, id = None):
        self._id = id
        self._ip_address = ip_address
        self._rack = rack
        self._slot = slot
        self._active = active
    
    #No ID since its unknown at this point
    def getModelAsRecordForInsert(self):
        return {"ip_address": self._ip_address, "rack": self._rack, "slot": self._slot, "active": self._active}

    def __str__(self) -> str:
        print(f'ID: {self._id}')
        print(f'Ip-address: {self._ip_address}')
        print(f'Rack: {self._rack}')
        print(f'Slot: {self._slot}')
        print(f'Active: {self._active}')