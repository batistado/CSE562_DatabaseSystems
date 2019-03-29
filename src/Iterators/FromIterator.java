package Iterators;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import Models.Schema;
import Models.TupleSchema;
import Utils.utils;
import dubstep.Main;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectItem;

public class FromIterator implements RAIterator{
	private BufferedReader reader;
	private ArrayList<PrimitiveValue> row;
	private String line;
	private Table table;
	private TupleSchema fromSchema;
	
	public FromIterator (Table table) {
		this.table = table;
		initializeReader();
		this.setIteratorSchema();
		//System.gc();
	}
	
	@Override
	public void resetWhere() {
		
	}
	
	@Override
	public void resetProjection() {
	}
	
	public void resetIterator() {
		initializeReader();
	}
	
	public void initializeReader() {
		try {
			reader = new BufferedReader(new FileReader(DIR + table.getName().toLowerCase() + ".csv"));
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		try {
				while (reader != null && (line = reader.readLine()) != null) {
					row = getLeftRow();
					return true;
				}
				
				if (reader != null) {
					row = null;
					reader.close();
					reader = null;
				}
				return false;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	public ArrayList<PrimitiveValue> getLeftRow(){
		String[] row = line.split("\\|");
		int j = 0;
		ArrayList<PrimitiveValue> tmp = new ArrayList<PrimitiveValue>();
		for(String x : row) {
			String colDatatype = Main.tableSchemas.get(table.getName()).getSchemaByIndex(j).getDataType();
			if(colDatatype.equals("string") || colDatatype.equals("varchar") || colDatatype.equals("char")) {
				StringValue val = new StringValue(x);
				tmp.add(val);
			}
			else if(colDatatype.equals("int")){
				LongValue val = new LongValue(x);
				tmp.add(val);
			}
			else if(colDatatype.equals("decimal")) {
				DoubleValue val = new DoubleValue(x);
				tmp.add(val);
			}
			else if(colDatatype.equals("date")){
				DateValue val = new DateValue(x);
				tmp.add(val);
			}
			
			j++;
			
		}
		
		return tmp;
	}
	
	public void setIteratorSchema() {
		String aliasedTableName = utils.getTableName(table);
		
		if (!aliasedTableName.equals(table.getName())) {
		// HERE mark
			fromSchema = new TupleSchema();
			Map<String, Schema> schemaByName = Main.tableSchemas.get(table.getName()).schemaByName();
			
			for (String name: schemaByName.keySet()) {
				// Since default columns are referenced as X.A
				String colName = aliasedTableName + "." + name.substring(name.lastIndexOf('.') + 1);
				Schema s = Main.tableSchemas.get(table.getName()).getSchemaByName(name);
				fromSchema.addTuple(colName, s.getColumnIndex(), s.getDataType());
			}
		}
		else {
			fromSchema = new TupleSchema();
			Map<String, Schema> schemaByName = Main.tableSchemas.get(table.getName()).schemaByName();
			
			for (String name: schemaByName.keySet()) {
				// Since default columns are referenced as X.A
				String strippedColName = name.substring(name.lastIndexOf('.') + 1);
				Schema s = Main.tableSchemas.get(table.getName()).getSchemaByName(name);
				fromSchema.addTuple(strippedColName, s.getColumnIndex(), s.getDataType());
				fromSchema.addTuple(name, s.getColumnIndex(), s.getDataType());
			}
		}
		
		// Try to push into HERE mark
		if (!aliasedTableName.equals(table.getName())) {
			// Add aliased schema to main tables schema. Helps for All Table Columns
			Main.tableSchemas.put(aliasedTableName, fromSchema);
		}
	}

	@Override
	public ArrayList<PrimitiveValue> next() {
		// TODO Auto-generated method stub
		return row;
	}

	@Override
	public TupleSchema getIteratorSchema() {
		// TODO Auto-generated method stub
		return fromSchema;
	}

	@Override
	public void addSelectItems(List<SelectItem> selectItems) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public TupleSchema getSelectSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RAIterator getLeftIterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RAIterator getRightIterator() {
		// TODO Auto-generated method stub
		return null;
	}
}
