import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;

import java.io.IOException;
import java.util.List;

/**
 * Created by fanzhongyu on 2017/6/12 0012.
 */
public class HbaseConnection {
    String hbaseDir;
    String zkServer;
    String zkPort;
    private Configuration conf;
    private Connection hConn;

    HbaseConnection(String hbaseDir,String zkServer,String zkPort) throws IOException {
        this.hbaseDir = hbaseDir;
        this.zkServer = zkServer;
        this.zkPort = zkPort;
        conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir",hbaseDir);
        conf.set("hbase.zookeeper.quorum",zkServer);
        conf.set("hbase.zookeeper.property.clientPort",zkPort);
        //hConn = ConnectionFactory.createConnection(conf);
        hConn = ConnectionFactory.createConnection(conf);
    }

    public void createTable(String tableName, List<String> cols) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if(admin.tableExists(tableName))
            throw new IOException("table exists");
        else{
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for(String col : cols) {
                HColumnDescriptor columnDesc = new HColumnDescriptor(col);
                columnDesc.setCompressionType(Compression.Algorithm.GZ);   //设置压缩方式
                columnDesc.setDataBlockEncoding(DataBlockEncoding.DIFF);
                tableDesc.addFamily(columnDesc);
            }
            admin.createTable(tableDesc);
        }
    }

    public void saveData(String tableName,List<Put> puts){

    }

    public Result getData(String tableName, String rowket){
        return null;
    }


}
