import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Administrator on 2017/6/12 0012.
 */
public class TestHbaseApi {
    public static void main (String[] args) throws IOException {
        String hbaseDir = "hdfs://valukacluster/hbase";
        String zkServer = "zk-1,zk-2,zk-3";
        String zkPort = "30000";
        HbaseConnection conn = new HbaseConnection(hbaseDir,zkServer,zkPort);
        List<String> cols = new LinkedList<String>();
        cols.add("baseinfo");
        cols.add("moreinfo");
        conn.createTable("student",cols);

    }
}
