// Lab 2 - FileSystemCat.java (excerpt from slide 117).
// Source: hadoop-book-code/ch03-hdfs (Tom White, Hadoop: The Definitive Guide).
// NOTE: the TaiLieuThamKhao/P2_LuuTruPhanTan/hadoop-book-code repo is not
// available on this machine, so this excerpt is copied from the slide.
// Import statements are omitted in the slide excerpt as well.
public class FileSystemCat {
  public static void main(String[] args) throws Exception {
    String uri = args[0];
    Configuration conf = new Configuration();
    // ^ loads cluster configuration (core-site.xml etc.)
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    // ^ contacts the NameNode to get metadata / block locations
    InputStream in = null;
    try {
      in = fs.open(new Path(uri));
      // ^ from here the data is read DIRECTLY from the DataNode
      IOUtils.copyBytes(in, System.out, 4096, false);
    } finally {
      IOUtils.closeStream(in);
    }
  }
}
