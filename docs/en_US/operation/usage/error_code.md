# Determine the error type by error code

In the result returned by eKuiper's REST API, if the internal processing of the REST request fails, an error code will be added to the returned error message.

## Error code type

| Error code | Description                                                                                  |
| ---------- | -------------------------------------------------------------------------------------------- |
| 1000       | Undefined error code, this error code means that the error was found to be undefined         |
| 1002       | Resource not found error code, this error code means that the required resource is not found |
| 1003       | IO error, this error code means that there is an IO error in Source/Sink                     |
| 1004       | Encoding error, this error means encoding error                                              |
| 2001       | SQL compilation error, this error means that the SQL does not conform to the syntax          |
| 2101       | SQL plan error, this error means that SQL cannot generate the execution plan correctly       |
| 2201       | SQL executor error, this error means that SQL cannot generate the executor correctly         |
| 3000       | Flow table error, this error means that a flow table related error occurred                  |
| 4000       | Rule error, this error means that a rule-related error occurred                              |
| 5000       | Configuration error, this error means that a configuration-related error occurred            |
