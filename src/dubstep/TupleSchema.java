package dubstep;

import java.util.HashMap;
import java.util.Map;

public class TupleSchema {
	private static Map<String, Schema> schemaByColumnName = new HashMap<String, Schema>();
	private static Map<Integer, Schema> schemaByColumnIndex = new HashMap<Integer, Schema>();
	
	public void addTuple(String colName, Integer index, String dataType) {
		Schema schema = new Schema(dataType, index, colName);
		if (!schemaByColumnName.containsKey(colName))
			schemaByColumnName.put(colName, schema);
		if (!schemaByColumnIndex.containsKey(index))
			schemaByColumnIndex.put(index, schema);
	}
	
	public Schema getSchemaByName(String colName) {
		if (schemaByColumnName.containsKey(colName)) {
			return schemaByColumnName.get(colName);
		}
		
		return null;
	}
	
	public Schema getSchemaByIndex(Integer colIndex) {
		if (schemaByColumnIndex.containsKey(colIndex)) {
			return schemaByColumnIndex.get(colIndex);
		}
		
		return null;
	}
}
