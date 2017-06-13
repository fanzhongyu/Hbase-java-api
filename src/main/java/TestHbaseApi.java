import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by fanzhongyu on 2017/6/12 0012.
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
        List<Put> puts = new LinkedList<Put>();
        Put put1 = new Put(Bytes.toBytes("Tom"));
        put1.addColumn(Bytes.toBytes("baseinfo"),Bytes.toBytes("age"),Bytes.toBytes("27"));
        put1.addColumn(Bytes.toBytes("moreinfo"),Bytes.toBytes("tel"),Bytes.toBytes("111"));
        Put put2 = new Put(Bytes.toBytes("Jim"));
        put2.addColumn(Bytes.toBytes("baseinfo"),Bytes.toBytes("age"),Bytes.toBytes("27"));
        put2.addColumn(Bytes.toBytes("moreinfo"),Bytes.toBytes("tel"),Bytes.toBytes("111"));
        puts.add(put1);
        puts.add(put2);
        conn.saveData("student",puts);
        Result result = conn.getData("student","Tom");
        conn.formatResult(result);

        conn.hbaseScan("student");
        conn.filterTest("student");
    }
}
