package com.vigilante.hbase.get;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
//import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.ResponseBody;

import com.vigilante.hbase.put.dataObj;

@Service
//@ImportResource("hbase-site.xml")
public class getService  {
	
	@Autowired
	private List<dataObj> ld;
	
	public @ResponseBody List<dataObj>  tableService() throws IOException {
		
		Configuration conf = HBaseConfiguration.create();
		//conf.set("hbase.zookeeper.quorum", "localhost");
        //conf.set("hbase.zookeeper.property.clientPort", "2181");

		//String p = this.getClass().getClassLoader().getResource("hbase-site.xml").getPath();
		//conf.addResource(new Path(p));
		
		 HBaseAdmin.available(conf);
	
        Connection con = ConnectionFactory.createConnection(conf);
        Table t = con.getTable(TableName.valueOf("covid"));
/*TableDescriptor td = con.getTable(TableName.valueOf("covid")).getDescriptor();
        System.out.println("Table check");        
        Set<byte[]> cfd = td.getColumnFamilyNames();
        for(Object x :cfd) {
        	System.out.println(Bytes.toString((byte[]) x));
        }
*/        
        Scan s = new Scan();
        ResultScanner rs = t.getScanner(s);        
        for (Result r : rs){
           
        	//System.out.println(r);
            CellScanner c = r.cellScanner();
            while (c.advance() ) {
            	
            	dataObj d = new dataObj();
                Cell cell = c.current();
                System.out.println(cell);
                
                byte[] rowId = Bytes.copy(cell.getRowArray(),cell.getRowOffset(), cell.getRowLength());
                //System.out.println(Bytes.toString(rowId));
                d.setRowkey(Bytes.toString(rowId));
                
                byte[] familyName = Bytes.copy(cell.getFamilyArray(),
                        cell.getFamilyOffset(),
                        cell.getFamilyLength());
                //System.out.println(Bytes.toString(familyName));
                d.setCfamily(Bytes.toString(familyName));
                
                byte[] columnName = Bytes.copy(cell.getQualifierArray(),
                        cell.getQualifierOffset(),
                        cell.getQualifierLength());
                //System.out.println(Bytes.toString(columnName));
                d.setCname(Bytes.toString(columnName));
				
                byte[] columnValue = Bytes.copy(cell.getValueArray(),
                        cell.getValueOffset(),
                        cell.getValueLength());
                //System.out.println(Bytes.toString(columnValue));
                d.setCvalue(Bytes.toString(columnValue));
                if( rowId != null && familyName!= null && columnName != null && columnValue != null) {
                	ld.add(d);
                }
             }
          
        }
        rs.close();
        return ld;
	}
	
	public @ResponseBody List<dataObj>  rowService(String table, String id) throws IOException {
		
		Configuration conf = HBaseConfiguration.create();
		 HBaseAdmin.available(conf);
	
        Connection con = ConnectionFactory.createConnection(conf);
        Table t = con.getTable(TableName.valueOf(table));
        Get g = new Get(id.getBytes());
        Result r = t.get(g);
        
        System.out.println(r);
        CellScanner c = r.cellScanner();
        while (c.advance() ) {
            	
            	dataObj d = new dataObj();
                Cell cell = c.current();
                System.out.println(cell);
                
                byte[] rowId = Bytes.copy(cell.getRowArray(),cell.getRowOffset(), cell.getRowLength());
                System.out.println(Bytes.toString(rowId));
                d.setRowkey(Bytes.toString(rowId));
                
                byte[] familyName = Bytes.copy(cell.getFamilyArray(),
                        cell.getFamilyOffset(),
                        cell.getFamilyLength());
                System.out.println(Bytes.toString(familyName));
                d.setCfamily(Bytes.toString(familyName));
                
                byte[] columnName = Bytes.copy(cell.getQualifierArray(),
                        cell.getQualifierOffset(),
                        cell.getQualifierLength());
                System.out.println(Bytes.toString(columnName));
                d.setCname(Bytes.toString(columnName));
				
                byte[] columnValue = Bytes.copy(cell.getValueArray(),
                        cell.getValueOffset(),
                        cell.getValueLength());
                System.out.println(Bytes.toString(columnValue));
                d.setCvalue(Bytes.toString(columnValue));
                if( rowId != null && familyName!= null && columnName != null && columnValue != null) {
                	ld.add(d);
                }
          
        }
       
        con.close();
        return ld;
        
	}
}
