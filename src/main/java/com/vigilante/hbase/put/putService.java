package com.vigilante.hbase.put;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.stereotype.Service;

@Service
public class putService {
	
	public dataObj insertService(dataObj dao) throws IOException {
		
	Configuration conf = HBaseConfiguration.create();
	conf.set("hbase.zookeeper.quorum", "localhost");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    //verify the hbase up and running
   HBaseAdmin.available(conf);
    Connection con = ConnectionFactory.createConnection(conf);
    Table t = con.getTable(TableName.valueOf("covid"));
    Put p = new Put(dao.getRowkey().getBytes());
    p.addColumn(dao.getCfamily().getBytes(),dao.getCname().getBytes(),dao.getCvalue().getBytes());
    t.put(p);
    con.close();
    return dao;
    
	}
}
