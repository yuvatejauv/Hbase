package com.vigilante.hbase.put;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class putController {
	
	@Autowired
	private putService ps;
	
	//@Autowired
	//private dataObj dao;
	
	putController(putService ps){
		this.ps = ps;
	}
	
	@RequestMapping(method=RequestMethod.POST,value="/covid")
	@ResponseBody
	public ResponseEntity<dataObj> insertRecord(@RequestBody dataObj dao ) throws IOException {
		//ps.insertService(dao);				
		return ResponseEntity.status(HttpStatus.OK).body(ps.insertService(dao));
	}
	
}
