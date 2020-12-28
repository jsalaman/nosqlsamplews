package com.example.restservice;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import oracle.nosql.driver.ops.*;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.Region;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.values.MapValue;


@RestController
public class GreetingController {

	private static final String template = "Hello, %s!";
	private final AtomicLong counter = new AtomicLong();
	
	String tableName = "DemoTable";
	String compartmentID = "ocid1.compartment.oc1..aaaaaaaand4qzwtujrzfqoboxb7bej2n3vjq5b6rgi5qa4iv64ssfzl6bhha";
	

	@GetMapping("/greeting")
	public Greeting greeting(@RequestParam(value = "name", defaultValue = "World") String name) {
		 try {
			NoSQLHandle handle = generateNoSQLHandle();
			readRows(handle);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new Greeting(counter.incrementAndGet(), String.format(template, name));
	}
	
	@GetMapping("/students")
	public List<Student> students(@RequestParam(value = "name", defaultValue = "World") String name) {
		List<Student> stu = new ArrayList<Student>();
		try {
			NoSQLHandle handle = generateNoSQLHandle();
			String query = "SELECT * FROM " + tableName;
	        PrepareRequest prepReq = new PrepareRequest().setStatement(query).setCompartment(compartmentID); 
	        PrepareResult prepRes = handle.prepare(prepReq); /* prepare statement */
	        QueryRequest queryRequest = new QueryRequest().setPreparedStatement(prepRes); /* set bind variable and query request */
	        do { /* Perform query until done */
	            QueryResult queryResult = handle.query(queryRequest);
	            //System.out.println("Reading values form table: \n" + tableName + "\n");
	            //System.out.println(queryResult.getResults()+"\n");
	            List<MapValue> res = queryResult.getResults();
	            for (MapValue mapValue : res) {
	            	stu.add(new Student(Long.valueOf(mapValue.getLong("studentid")),mapValue.getString("name")));
				}
	        } while (!queryRequest.isDone()); /* handle result */
	        
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return stu;
	}
	
	@GetMapping("/createTable")
	public Status createTable(@RequestParam(value = "name", defaultValue = "World") String name) {
		NoSQLHandle handle;
		try {
			handle = generateNoSQLHandle();
			createTable(handle);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return new Status("nok","tabla NO creada:"+ e.getMessage());
		}
		
		return new Status("ok","tabla creada");
	}
	
	@GetMapping("/dropTable")
	public Status dropTable(@RequestParam(value = "name", defaultValue = "World") String name) {
		NoSQLHandle handle;
		try {
			handle = generateNoSQLHandle();
			dropTable(handle);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return new Status("nok","tabla NO eliminada:"+ e.getMessage());
		}
		
		return new Status("ok","tabla eliminada");
	}
	
	@GetMapping("/insertRow")
	public Status insertRow(@RequestParam(value = "studentid", defaultValue = "1") String studentId, 
			@RequestParam(value = "name", defaultValue = "World") String name) {
		System.out.println(name);
		System.out.println(studentId);
		NoSQLHandle handle;
		try {
			handle = generateNoSQLHandle();
			writeRows(handle, studentId, name);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return new Status("nok","row NO creada:"+ e.getMessage());
		}
		
		return new Status("ok","row creada");
	}
	
	
	void writeRows(NoSQLHandle handle, String studentid, String name) {
        MapValue value = new MapValue().put("studentid", studentid).put("name", name);
        System.out.println("Inserting a row into " + tableName);
        PutRequest putRequest = new PutRequest().setValue(value).setTableName(tableName).setCompartment(compartmentID);
        PutResult putResult = handle.put(putRequest);
        if (putResult.getVersion() != null){
            System.out.println("Inserted " + value);
            System.out.println("Finishing inserting records");
        } else {
            System.out.println("Put failed");
        }       
    }
	
	public void readRows(NoSQLHandle handle) {
        String query = "SELECT * FROM " + tableName;
        PrepareRequest prepReq = new PrepareRequest().setStatement(query).setCompartment(compartmentID); 
        PrepareResult prepRes = handle.prepare(prepReq); /* prepare statement */
        QueryRequest queryRequest = new QueryRequest().setPreparedStatement(prepRes); /* set bind variable and query request */
        do { /* Perform query until done */
            QueryResult queryResult = handle.query(queryRequest);
            System.out.println("Reading values form table: \n" + tableName + "\n");
            System.out.println(queryResult.getResults()+"\n");
            List<MapValue> res = queryResult.getResults();
            for (MapValue mapValue : res) {
            	System.out.println(mapValue.get("studentid"));
            	System.out.println(mapValue.get("name"));
			}
        } while (!queryRequest.isDone()); /* handle result */
        System.out.println("Finishing listing records");
    }
	
	public NoSQLHandle generateNoSQLHandle() throws Exception {
        SignatureProvider ap = new SignatureProvider();
        System.out.println("Connecting to OCI NoSQL..... ");
        NoSQLHandleConfig config = new NoSQLHandleConfig(Region.US_ASHBURN_1, ap); /* set Region according to your tenant location */
        config.setAuthorizationProvider(ap);
        NoSQLHandle handle = NoSQLHandleFactory.createNoSQLHandle(config);
        return handle;
    }
	
	public void createTable(NoSQLHandle handle) throws Exception {
        String createTableDDL = "CREATE TABLE IF NOT EXISTS " +
            tableName + "(studentid INTEGER, name STRING, " +
            "PRIMARY KEY(studentid))";
        
        TableLimits limits = new TableLimits(10, 20, 2);
        TableRequest treq = new TableRequest()
            .setCompartment(compartmentID).setStatement(createTableDDL).setTableLimits(limits);
        
        System.out.println("Creating table " + tableName);
        TableResult tres = handle.tableRequest(treq); /* request is async... wait for it */

        System.out.println("Waiting for " + tableName + " to become active");
        tres.waitForCompletion(handle, 60000, 1000); /* 60 sec wait, 1 sec poll */
        System.out.println("Table " + tableName + " created");
        treq = new TableRequest().setCompartment(compartmentID).setStatement("CREATE INDEX IF NOT EXISTS nameIdx ON DemoTable(studentid)");
        handle.tableRequest(treq);
        tres.waitForCompletion(handle, 60000, 1000); /* 60 sec wait, 1 sec poll */
        System.out.println("Index nameIdx created");
    }
	
	public void dropTable(NoSQLHandle handle) throws Exception {
        String dropTableDDL = "DROP TABLE " + tableName;
        TableRequest treq = new TableRequest()
            .setCompartment(compartmentID).setStatement(dropTableDDL);
        
        System.out.println("Dropping table " + tableName);
        TableResult tres = handle.tableRequest(treq); /* request is async... wait for it */

        System.out.println("Waiting for " + tableName + " to be deleted");
        tres.waitForCompletion(handle, 60000, 1000); /* 60 sec wait, 1 sec poll */
        System.out.println("Table " + tableName + " deleted");
    }
}
