package com.vigilante.hbase.put;

import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

@Component
@Repository
public class dataObj {
	
	private String rowkey;
	private String cfamily;
	private String cname;
	private String cvalue;
	
	public dataObj () {
	
	}
	
	public dataObj (String rowkey, String cfamily, String cname, String cvalue) {
		this.rowkey = rowkey;
		this.cfamily = cfamily;
		this.cname = cname;
		this.cvalue = cvalue;
	}
	
	public String getRowkey() {
		return rowkey;
	}
	public void setRowkey(String rowkey) {
		this.rowkey = rowkey;
	}
	public String getCfamily() {
		return cfamily;
	}
	public void setCfamily(String cfamily) {
		this.cfamily = cfamily;
	}
	public String getCname() {
		return cname;
	}
	public void setCname(String cname) {
		this.cname = cname;
	}
	public String getCvalue() {
		return cvalue;
	}
	public void setCvalue(String cvalue) {
		this.cvalue = cvalue;
	}
	

}
