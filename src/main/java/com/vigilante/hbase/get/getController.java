package com.vigilante.hbase.get;

import java.io.IOException;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import com.vigilante.hbase.put.dataObj;


@RestController
public class getController {
	
	@Autowired
	private getService gs;
	/*@Autowired
	private dataObj d;
	
	private List<dataObj> ld;
	*/
	getController(getService gs){
		this.gs =gs;
	}
	
	//Read the full table
	@RequestMapping(value="/covid",method=RequestMethod.GET)
	@ResponseBody
	public List<dataObj> readTable() throws IOException  {
		return (List<dataObj>) gs.tableService();		
	}
	
	//Read the one row
	@RequestMapping(value="/rowkey/{table}/{id}",method=RequestMethod.GET)
	@ResponseBody
	public ResponseEntity<List<dataObj>> readRow( @PathVariable(value="table", required=true) String table, 
			@PathVariable(value="id", required=true) String id ) throws IOException  {
		//return (List<dataObj>) gs.rowService(table, id);	
		return ResponseEntity.status(HttpStatus.OK).body(gs.rowService(table, id));
	}
}
