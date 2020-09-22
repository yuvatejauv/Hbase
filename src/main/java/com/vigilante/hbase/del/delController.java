package com.vigilante.hbase.del;


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
public class delController {
		
		@Autowired
		private delService ds;
		
		delController(delService ds){
			this.ds =ds;
		}
		
		//disable the full table
		@RequestMapping(value="/disable/{table}",method=RequestMethod.GET)
		@ResponseBody
		public String delTable(@PathVariable(value="table", required=true) String table) throws IOException  {
			return ds.disableTable(table);		
		}
		
		//enable the full table
		@RequestMapping(value="/enable/{table}",method=RequestMethod.GET)
		@ResponseBody
		public String enableTable(@PathVariable(value="table", required=true) String table) throws IOException  {
			return ds.enableTable(table);		
		}
		
		//delete one row
		@RequestMapping(value="/delete/{table}",method=RequestMethod.DELETE)
		@ResponseBody
		public ResponseEntity<String> deleteRow( 
				@PathVariable(value="table", required=true) String table) throws IOException  {	
			return ResponseEntity.status(HttpStatus.OK).body(ds.deleteTable(table));
		}
		
		//delete one row
		@RequestMapping(value="/delete/{table}/{id}",method=RequestMethod.DELETE)
		@ResponseBody
		public ResponseEntity<String> deleteRow( @PathVariable(value="table", required=true) String table, 
				@PathVariable(value="id", required=true) String id ) throws IOException  {
			//return (List<dataObj>) gs.rowService(table, id);	
			return ResponseEntity.status(HttpStatus.OK).body(ds.deleteRow(table, id));
		}
		
		//delete one row's column family
		@RequestMapping(value="/delete/{table}/{id}/{cf}",method=RequestMethod.DELETE)
		@ResponseBody
		public ResponseEntity<String> deleteCf( @PathVariable(value="table", required=true) String table, 
				@PathVariable(value="id", required=true) String id,
				@PathVariable(value="cf", required=true) String cf ) throws IOException  {
			//return (List<dataObj>) gs.rowService(table, id);	
			return ResponseEntity.status(HttpStatus.OK).body(ds.deleteColFamily(table, id,cf));
		}
		
		//delete one qualifier from the column family of particular row
		@RequestMapping(value="/delete/{table}/{id}/{cf}/{qalifier}",method=RequestMethod.DELETE)
		@ResponseBody
		public ResponseEntity<String> deleteQalifier( @PathVariable(value="table", required=true) String table, 
						@PathVariable(value="id", required=true) String id,
						@PathVariable(value="cf", required=true) String cf,
						@PathVariable(value="qalifier", required=true) String qalifier) throws IOException  {	
			return ResponseEntity.status(HttpStatus.OK).body(ds.deleteQualifier(table, id,cf,qalifier));
		}
	}

