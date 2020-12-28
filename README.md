# nosqlsamplews

Sample endpoint calls:

curl http://localhost:8080/createTable -> Create a table named DemoTable
curl http://localhost:8080/dropTable -> Drop a table named DemoTable
curl "http://localhost:8080/insertRow?studentid=2&name=luis" -> Insert a row into DemoTable
curl http://localhost:8080/students -> List all students in DemoTable
