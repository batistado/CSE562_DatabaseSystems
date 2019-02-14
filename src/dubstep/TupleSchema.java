package dubstep;

import java.util.HashMap;
import java.util.Map;

public class TupleSchema {
	private static Map<String, Integer> schema = new HashMap<String, Integer>();
	
	public void addTuple(String colName, Integer index) {
		if (!schema.containsKey(colName))
			schema.put(colName, index);
	}
	
	public Integer getIndex(String colName) {
		if (schema.containsKey(colName)) {
			return schema.get(colName);
		}
		
		return null;
	}
}
