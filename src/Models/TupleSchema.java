package Models;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;

public class TupleSchema implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Map<String, Schema> schemaByColumnName = new HashMap<String, Schema>();
	private Map<Integer, Schema> schemaByColumnIndex = new HashMap<Integer, Schema>();
	private Map<String, Integer> dataTypeMap = new HashMap<String, Integer>();
	
	public TupleSchema() {
		dataTypeMap.put("string", 1);
		dataTypeMap.put("varchar", 1);
		dataTypeMap.put("char", 1);
		dataTypeMap.put("int", 2);
		dataTypeMap.put("decimal", 3);
		dataTypeMap.put("date", 4);
	}
	
	public void addTuple(String colName, Integer index, String dataType) {
		Integer dataTypeValue = dataTypeMap.get(dataType);
		Schema schema = new Schema(dataTypeValue, index, colName);
		schemaByColumnName.put(colName, schema);
		schemaByColumnIndex.put(index, schema);
	}
	
	public void addTuple(String colName, Integer index, Integer dataType) {
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
	
	public boolean containsKey(String key) {
		return schemaByColumnName.containsKey(key);
	}
	
	public boolean containsKey(Integer key) {
		return schemaByColumnIndex.containsKey(key);
	}

	public void updateTuple(String colName, Integer index, String dataType) {
		Integer dataTypeValue = dataTypeMap.get(dataType);
		Schema schema = new Schema(dataTypeValue, index, colName);
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
