"""Lab 2: read real HDFS Java API code and map it to the cluster architecture.

The task (slide 116) is to open hadoop-book-code/ch03-hdfs and identify, in
FileSystemCat.java / URLCat.java, which line loads the cluster configuration,
which line contacts the NameNode, and where the actual data read happens.

NOTE (honesty): the TaiLieuThamKhao/P2_LuuTruPhanTan/hadoop-book-code repo is
NOT available on this machine, so we use the FileSystemCat.java excerpt copied
from slide 117 (saved next to this script) and analyze that excerpt.
"""

import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JAVA_FILE = os.path.join(BASE_DIR, "FileSystemCat.java")


def main():
    print("Lab 2 - Reading the HDFS Java API (FileSystemCat)")
    print("NOTE: hadoop-book-code is not available locally; analysis below uses")
    print("the FileSystemCat.java excerpt from slide 117.")
    print()

    with open(JAVA_FILE, encoding="ascii") as f:
        source = f.read()
    print("--- File content (FileSystemCat.java excerpt) ---")
    print(source)
    print()

    print("--- Line-by-line analysis ---")
    print("1. Configuration conf = new Configuration();")
    print("   -> loads the cluster configuration (core-site.xml etc.) so the")
    print("      client knows which NameNode/filesystem to talk to.")
    print("2. FileSystem fs = FileSystem.get(URI.create(uri), conf);")
    print("   -> THIS line contacts the NameNode: it resolves the HDFS URI and")
    print("      fetches metadata + block locations. No file data flows yet.")
    print("3. in = fs.open(new Path(uri));")
    print("   -> opens the file; from this point the client reads data DIRECTLY")
    print("      from the DataNodes that hold the blocks (NameNode not in the")
    print("      data path anymore).")
    print("4. IOUtils.copyBytes(in, System.out, 4096, false);")
    print("   -> streams the block bytes to stdout with a 4096-byte buffer.")
    print()

    print("--- Mapping to the architecture seen in class ---")
    print("NameNode = master: stores metadata + block locations, never serves")
    print("           file bytes. DataNode = worker: stores and serves blocks.")
    print("Same master/worker split as GFS: NameNode ~ master, DataNode ~")
    print("chunkserver; clients read/write block data straight to the workers.")


if __name__ == "__main__":
    main()
