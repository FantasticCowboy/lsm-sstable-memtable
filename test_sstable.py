from sstable import SSTable
import tempfile
import pathlib
import pdb
import time

def main():
    def test_can_create_and_read():
        print("testing can create and read")
        with tempfile.TemporaryDirectory() as tmpdir:
            path = pathlib.Path(tmpdir)
            data = {
                "a" : "aasdad",
                "b" : "b",
                "c" : "c",
            }
            print("writing data")
            SSTable.write(data, path)

            print("creating sstable object")
            sstable = SSTable(path)

            print("testing sstable get")
            assert sstable.get("a") == "aasdad", sstable.get("a")
            assert sstable.get("b") == "b", sstable.get("b")
            assert sstable.get("c") == "c", sstable.get("c")
    
    def test_can_index_and_readkjv():
        print("testing can create and read")
        line_num_to_line = {}

        with open("kjv.txt", "rt", encoding="utf-8") as kjv_fp:
            for i, line in enumerate(kjv_fp):
                line_num_to_line[str(i)] = line

        with tempfile.TemporaryDirectory() as tmpdir:
            path = pathlib.Path(tmpdir)
            print("writing data")
            SSTable.write(line_num_to_line, path)

            print("creating sstable object")
            sstable = SSTable(path)

            print("testing sstable get")
            
            start_time = time.time()            
            for key, value in line_num_to_line.items():
                assert sstable.get(key) == value, sstable.get(key)
            print(f"total time {time.time() - start_time}")
            pdb.set_trace()

    test_can_create_and_read()
    test_can_index_and_readkjv()

            


if __name__ == "__main__":
    main()