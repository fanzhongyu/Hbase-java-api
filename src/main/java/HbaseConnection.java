import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;

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
        //HBaseAdmin admin = new HBaseAdmin(conf);
        Admin admin = hConn.getAdmin();
        if(admin.tableExists(TableName.valueOf(tableName))) {
            admin.close();
            throw new IOException("table exists");
        }
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

    public void saveData(String tableName,List<Put> puts) throws IOException {
        Table table = hConn.getTable(TableName.valueOf(tableName));
        table.put(puts);
        table.close();
    }

    public Result getData(String tableName, String rowkey) throws IOException {
        Table table = hConn.getTable(TableName.valueOf(tableName));
        try {
            Get get = new Get(Bytes.toBytes(rowkey));
            Result result = table.get(get);
            return result;
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            table.close();
        }
        return null;
    }

    public void formatResult(Result result){
        String rowKey = Bytes.toString(result.getRow());
        System.out.println("rowKey -> "+rowKey);
        Cell[] cells = result.rawCells();
        for(Cell cell : cells){
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println("columnfamily -> "+family+":"+qualifier+"  value -> "+value);
        }
        System.out.println("*********************************");
    }

    public void hbaseScan(String tableName) throws IOException {
        Scan scan =new Scan();
        scan.setCaching(1000);
        Table table = hConn.getTable(TableName.valueOf(tableName));
        ResultScanner scanner = table.getScanner(scan);
        for(Result result : scanner){
            formatResult(result);
        }
    }
}
