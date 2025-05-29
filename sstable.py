import pathlib
from asyncio import StreamReader
from io import TextIOWrapper, FileIO
from itertools import pairwise

NULL = "\0"
INDEX_FILE = "sstable.index"
VALUES_FILE = "sstable.values"
UTF_8 = "utf-8"

class SSTable:
    """
    SSTable for reading and writing key-value pairs.

    The SSTable is split into two files the index file and the key-value pairs file.

    The index contains key to file offset mappings. It is read into memory upon creation
        of the SSTable.
    The second section contains the values.

    An SSTable is immutable once created, meaning that it cannot be modified.

    The keys and values of the SSTable are encoded as UTF-8 strings.

    Keys are unique.

    Keys can only be alpha-numeric and values can be any characer accept the null terminator

    """

    def __init__(self, directory : pathlib.Path):        
        self._index : dict[str, tuple[int,int]] = self._load_index(directory / INDEX_FILE)
        self._values_files = directory / VALUES_FILE

    def get(self, key : str) -> str | None:
        if not key in self._index:
            return None
        
        offset, size = self._index[key]

        with open(self._values_files, mode="rb") as values_fp:
            values_fp : FileIO
            values_fp.seek(offset, 0)
            return values_fp.read(size).decode(encoding="utf-8")         
    

    def _load_index(self, index : pathlib) -> dict[str, tuple[int,int]]:
        with open(index, mode="rb") as index_fp:
            index_fp : FileIO
            raw_bytes = index_fp.read()
        
        index : dict[str, tuple[int,int]] = {}
        
        i = 0
        key_start = 0
        key_offset = []
        while i < len(raw_bytes):
            if chr(raw_bytes[i]) == "\0":                
                key = raw_bytes[key_start:i].decode("utf-8")
                offset = int.from_bytes(raw_bytes[i + 1 : i + 5])                
                
                key_offset.append((offset,key))                
                
                key_start = i + 5
                i = i + 5
            i += 1

        key_offset.sort()

        
        for (offset, key), (next_offset, next_key)in pairwise(key_offset):
            index[key] = (offset, next_offset - offset)
        
        index[key_offset[-1][1]] = (key_offset[-1][0], -1)
                
        return index

    @classmethod
    def write(cls, data : dict[str : str], directory : pathlib.Path) -> "SSTable":

        with ( 
            open(directory / INDEX_FILE, mode="wb") as index_fp, 
            open(directory / VALUES_FILE, mode="wb") as values_fp 
        ):
            offset = 0
            for key, value in data.items():
                key : str
                value : str
                if not key.isalnum():
                    raise TypeError("Keys must be alpha numeric")
                if "\0" in value:
                    raise TypeError("Value cannot contain the null string \\0")
                
                index_fp.write(key.encode(encoding=UTF_8))
                index_fp.write("\0".encode(encoding=UTF_8))
                index_fp.write(offset.to_bytes(length=4))

                encoded_value = value.encode(encoding=UTF_8)
                values_fp.write(encoded_value)

                offset += len(encoded_value)
