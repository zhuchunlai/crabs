Crabs is a SQL-like JDBC driver and command line for elasticsearch(v1.2.2). It follows the SQL-92 specification, and we introduce some appropriate adjustments based on the features of elasticsearch. Crabs is very simple for users, we provide JDBC driver. With it you may use elasticsearch as simply as using SQL with traditional database.

__NOTE:__ When you use crabs, there are some restrictions on elasticsearch index and type.

	name of index and type: must follow java identifier specification, that are:
		
		the first character is must be "_", "$" or english character.
		
		the other characters are must be digit, "_", "$" or english characters.
		
	field name of type: it is the same of above.
	
	all names are case sensitive
	
	you must set "_id" mapping and be associated with a path, need more details, please click [link](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/mapping-id-field.html)
 
## Documentation
Now, crabs support two types of select statement, detail as bellow:

__NOTE:__ SQL is case sensitive

### Non-aggregation query SQL
    SELECT {derived column list} 
        FROM type
		[WHERE {where condition}]
		[HAVING {having condition}]
		[ORDER BY order-specifications] 
		[LIMIT offset, rowCount]
		
#### where
	{derived column list}: '*' or comma-separated list of field names or constant and they can have aliases
	{where condition}: 
		{field name} [>, >=, <, <=, ==, <>] value
		{field name} like {pattern}
		{field name} not like {pattern}
		{field name} in(value1, value2, ...)
		{field name} not in(value1, value2, ...)
		{field name} between min-value and max-value
		{field name} not between min-value and max-value
		{where condition} and {where condition}
		{where condition} or {where condition}
	{having search}:
	    similar to {where condition}, but field name is replaced with derived column name
	{order-specifications}:
	    comma-separated list of {derived column name ASC|DESC}
	{offset}:
	    start index of query result to returned, similar to "from" of elasticsearch
	{rowCount}:
	    max count of rows to returned, similar to "size" of elasticsearch
	{pattern}:
		consist of '%' and string value.
	    
__NOTE:__ Now, crabs does not support scroll, so, when the query result is too large, it will be truncated. The defualt critical value is 500, of cause, you can set the property(clientScanSize) when you create a JDBC connection to change the default value.
	
### Aggregation query SQL
	SELECT {aggregation expression list}|{group by column list}|{value}
		FORM type
		[WHERE {where condition}]
		[GROUP BY {group column list}]
		
#### where
	{aggregation expression list}: 
		comma-separated list of aggregate functions and they can have aliases
	{group by column list}: 
		comma-separated list of field names, and the field must be exist in group column list.
	{where condition}: 
		similar to "where condition" of Non-aggregation query SQL
	{group column list}: 
		comma-separated list of field names
	{aggregate functions}: 
		sum('field name'), count('*'|'field name'), avg('field name'), max('field name'), min('field name')
	    
### DataType
    double, float, long, int, boolean, String, Date
    
__NOTE:__ For the date type, crabs also supports user-defined date format, only need to follow java date format pattern specification.

### JDBC URL
	jdbc:crabs://{elasticsearch address}[{, elasticsearch address}...]/indexName[?{propertyName=propertyValue}][{&propertyName=propertyValue}...]
	
#### where
	{elasticsearch address}	: ip:port
	
#### properties
Crabs has two system properties. Details as bellow:

	clientScanSize: define the max count of rows returned per query and only works on Non-aggregation query SQL
	
	metaDataTTL: define the ttl of meta data(index and type meta data) in crabs, its unit is minute.
		 
## More

Details about elasticsearch, http://www.elasticsearch.org	    
