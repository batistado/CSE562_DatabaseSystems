package Models;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TupleSchema {
	private Map<String, Schema> schemaByColumnName = new HashMap<String, Schema>();
	private Map<Integer, Schema> schemaByColumnIndex = new HashMap<Integer, Schema>();
	
	public void addTuple(String colName, Integer index, String dataType) {
		Schema schema = new Schema(dataType, index, colName);
		schemaByColumnName.put(colName, schema);
		schemaByColumnIndex.put(index, schema);
	}
	
	public List<Integer> getIndexesOfTable(String tableName){
		ArrayList<Integer> result = new ArrayList<>();
		for (String columnName: schemaByColumnName.keySet()) {
			if (columnName.startsWith(tableName))
				result.add(schemaByColumnName.get(columnName).getColumnIndex());
		}
		
		Collections.sort(result);
		return result;
	}

	public void updateTuple(String colName, Integer index, String dataType) {
		Schema schema = new Schema(dataType, index, colName);
		schemaByColumnName.put(colName, schema);
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
	
	public Map<String, Schema> schemaByName() {
		return schemaByColumnName;
	}
	
	public Map<Integer, Schema> schemaByIndex() {
		return schemaByColumnIndex;
	}
}
