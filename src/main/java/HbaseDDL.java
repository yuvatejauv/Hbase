import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;

public  class HbaseDDL  {

    static Configuration conf = HBaseConfiguration.create();

    public static void main (String[] args )  throws IOException
    {
       // conf.set("zookeeper.znode.parent", "/hbase");
        //conf.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
        //conf.setBoolean("hbase.cluster.distributed", true);
        //conf.setInt("hbase.client.scanner.caching", 10000);
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        Connection con = ConnectionFactory.createConnection(conf);
        Table t = con.getTable(TableName.valueOf("covid"));
        System.out.println("Table check");

        Scan s = new Scan();
        ResultScanner rs = t.getScanner(s);


        for (Result r : rs){
            //System.out.println(r);
            System.out.println(Bytes.toString(r.getRow()));
            System.out.println("geo.Country_Region: " + Bytes.toString(r.getValue("geo".getBytes(),"Country_Region".getBytes() ) ));
            System.out.println("geo.Province_State: " +Bytes.toString(r.getValue("geo".getBytes(),"Province_State".getBytes() ) ));
            System.out.println("geo.Last_Update: " +Bytes.toString(r.getValue("log".getBytes(),"Last_Update".getBytes() ) ));
            System.out.println("geo.Country_Region: " +Bytes.toString(r.getValue("log".getBytes(),"Country_Region".getBytes() ) ));
        }
        rs.close();

        Put p = new Put("5".getBytes());
        p.addColumn("geo".getBytes(),"Country_Region".getBytes(),"Italy".getBytes());
        t.put(p);

    }
}
