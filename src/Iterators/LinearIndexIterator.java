package Iterators;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import Indexes.Position;
import Models.Schema;
import Models.TupleSchema;
import Utils.utils;
import dubstep.Main;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectItem;

public class LinearIndexIterator implements RAIterator{
	private BufferedReader reader;
	private ArrayList<PrimitiveValue> row;
	private Table table;
	private TupleSchema fromSchema;
	private Expression where = null;
	private List<Position> positions;
	private long currPosition;
	private int currIndex;
	private FileInputStream fis;
	
	public LinearIndexIterator (Table table, Expression where, List<Position> positions) {
		this.positions = positions;
		this.table = table;
		resetIterator();
		this.where = where;
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
		currIndex = 0;
		currPosition = positions.get(currIndex).startPosition;
		initializeReader();
	}
	
	public void initializeReader() {
		try {
			fis = new FileInputStream(DIR + table.getName() + ".csv");
			fis.getChannel().position(currPosition);
			InputStreamReader isr = new InputStreamReader(fis);
			reader = new BufferedReader(isr);
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	
	private void seekToPosition(long newPosition) {
		try {
			fis.getChannel().position(newPosition);
			InputStreamReader isr = new InputStreamReader(fis);
			reader = new BufferedReader(isr);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		try {
				while (reader != null && currIndex < positions.size() && currPosition <= positions.get(currIndex).endPosition && (row = getLeftRow(reader.readLine())) != null) {
					if (where == null || utils.filterRow(row, where, fromSchema))
						return true;
				}
				
				while (currIndex + 1 < positions.size()) {
					currIndex++;
					currPosition = positions.get(currIndex).startPosition;
					seekToPosition(currPosition);
					
					while (reader != null && currIndex < positions.size() && currPosition <= positions.get(currIndex).endPosition && (row = getLeftRow(reader.readLine())) != null) {
						if (where == null || utils.filterRow(row, where, fromSchema))
							return true;
					}
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
	
	public ArrayList<PrimitiveValue> getLeftRow(String line){
		if (line == null) {
			return null;
		}
		
		currPosition += line.length() + Main.offset;
		String[] row = line.split("\\|");
		int j = 0;
		ArrayList<PrimitiveValue> tmp = new ArrayList<PrimitiveValue>();
		for(String x : row) {
			Integer colDatatype = Main.tableSchemas.get(table.getName()).getSchemaByIndex(j).getDataType();
			if(colDatatype == 1) {
				StringValue val = new StringValue(x);
				tmp.add(val);
			}
			else if(colDatatype == 2){
				LongValue val = new LongValue(x);
				tmp.add(val);
			}
			else if(colDatatype == 3) {
				DoubleValue val = new DoubleValue(x);
				tmp.add(val);
			}
			else if(colDatatype == 4){
				DateValue val = new DateValue(x);
				tmp.add(val);
			}
			
			j++;
			
		}
		
		return tmp;
	}
	
	public void setIteratorSchema() {
		//String aliasedTableName = utils.getTableName(table);
		String aliasedTableName = table.getAlias() == null ? table.getName() : table.getAlias();
		
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


