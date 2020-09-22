package com.vigilante.hbase.del;

import java.io.IOException;
//import com.vigilante.hbase.put.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.stereotype.Service;

@Service
public class delService {
	
	public String deleteRow(String table, String id) throws IOException {
		
	Configuration conf = HBaseConfiguration.create();
	conf.set("hbase.zookeeper.quorum", "localhost");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    //verify the hbase up and running
    	HBaseAdmin.available(conf);
    	Connection con = ConnectionFactory.createConnection(conf);
    	Table t = con.getTable(TableName.valueOf(table)); 
    	Delete d = new Delete(id.getBytes());
    	t.delete(d);
    	con.close();
    	return  "All the column familes and column qualifiers of the row " + id + " is deleted successfully";
	}
	
	public String deleteColFamily(String table, String id, String colFamily) throws IOException {
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "localhost");
	    conf.set("hbase.zookeeper.property.clientPort", "2181");
	    //verify the hbase up and running
	   HBaseAdmin.available(conf);
	    Connection con = ConnectionFactory.createConnection(conf);
	    Table t = con.getTable(TableName.valueOf(table)); 
	    Delete d = new Delete(id.getBytes());
	    d.addFamily(colFamily.getBytes());
	    t.delete(d);
	    con.close();
	    return  "All the column qualifiers from column familes of the row " + id + " is deleted successfully";
	}
	
	public String deleteQualifier(String table, String id, String colFamily, String qalifier) throws IOException {
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "localhost");
	    conf.set("hbase.zookeeper.property.clientPort", "2181");
	    //verify the hbase up and running
	   HBaseAdmin.available(conf);
	    Connection con = ConnectionFactory.createConnection(conf);
	    Table t = con.getTable(TableName.valueOf(table)); 
	    Delete d = new Delete(id.getBytes());
	    d.addColumn(colFamily.getBytes(), qalifier.getBytes());
	    t.delete(d);
	    con.close();
	    return  "The column qualifier" + qalifier +" from column family" + colFamily +" of the row " + id + " is deleted successfully";
	}
	
	public String deleteTable(String table) throws IOException {
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "localhost");
	    conf.set("hbase.zookeeper.property.clientPort", "2181");
	    //verify the hbase up and running
	    HBaseAdmin.available(conf);
	    Connection con = ConnectionFactory.createConnection(conf);
	    if(con.getAdmin().isTableDisabled(TableName.valueOf(table))) {
	    	System.out.println("Table " + table +"  is already disabled,so ready to delete");
	    	con.getAdmin().deleteTable(TableName.valueOf(table));
	    	con.close();
	    	return  "The table " + table +" is deleted successfully";
	    }
	    else {
	    	System.out.println("Table " + table +"  is not disabled");
	    	con.close();
	    	return  "The Deletion of the table " + table +" is failed";
	    }
	}
	
	public String disableTable(String table) throws IOException {
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "localhost");
	    conf.set("hbase.zookeeper.property.clientPort", "2181");
	    //verify the hbase up and running
	   HBaseAdmin.available(conf);
	    Connection con = ConnectionFactory.createConnection(conf);
	    Table t = con.getTable(TableName.valueOf(table)); 
	    if( con.getAdmin().isTableDisabled(TableName.valueOf(table)) ) {
	    	System.out.println("Table is already disabled");
	    	con.close();
	 	    return  "The table " + table +" is already disabled";
	    }
	    else {
	    	con.getAdmin().disableTable(TableName.valueOf(table));
	    	System.out.println("Table is is disabled now");
	    	con.close();
	 	    return  "The table " + table +" is disabled now";
	    }
	    	   
	}
	
	public String enableTable(String table) throws IOException {
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "localhost");
	    conf.set("hbase.zookeeper.property.clientPort", "2181");
	    //verify the hbase up and running
	   HBaseAdmin.available(conf);
	    Connection con = ConnectionFactory.createConnection(conf);
	    Table t = con.getTable(TableName.valueOf(table)); 
	    if( con.getAdmin().isTableDisabled(TableName.valueOf(table)) ) {
	    	con.getAdmin().enableTable(TableName.valueOf(table));
	    	con.close();
	 	    return  "The table " + table +" is enabled now";
	    }
	    else {
	    	System.out.println("Table is already enabled");
	    	con.close();
	 	    return  "The table " + table +" is already enabled";
	    }
	    	   
	}
}
