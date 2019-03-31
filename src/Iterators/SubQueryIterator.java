package Iterators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import Models.Schema;
import Models.TupleSchema;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.SelectItem;

public class SubQueryIterator implements RAIterator {
	private RAIterator rightIterator = null;
	private ArrayList<PrimitiveValue> row;
	private TupleSchema fromSchema;
	private String alias;
	
	public SubQueryIterator(RAIterator rightIterator, String alias) {
		this.rightIterator = rightIterator;
		this.alias = alias;
		setIteratorSchema();
		//System.gc();
	}
	
	public void setIteratorSchema() {
		fromSchema = rightIterator.getIteratorSchema();
		
		TupleSchema rightIteratorSchema = rightIterator.getIteratorSchema();
		fromSchema = new TupleSchema();
		
		Map<String, Schema> schemaByName = rightIteratorSchema.schemaByName();
		for (String name: schemaByName.keySet()) {
			String colName = name;
			Schema s = rightIteratorSchema.getSchemaByName(name);
			fromSchema.addTuple(colName, s.getColumnIndex(), s.getDataType());
			
			if (alias != null) {
				fromSchema.addTuple(alias + "." + colName, s.getColumnIndex(), s.getDataType());
			}
		}
	}
	
	public String getAlias() {
		return this.alias;
	}

	@Override
	public void resetWhere() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public TupleSchema getIteratorSchema() {
		// TODO Auto-generated method stub
		return fromSchema;
	}

	@Override
	public void resetIterator() {
		// TODO Auto-generated method stub
		rightIterator.resetIterator();
	}

	@Override
	public void resetProjection() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ArrayList<PrimitiveValue> next() {
		// TODO Auto-generated method stub
		return row;
	}

	@Override
	public boolean hasNext() {
		if (!rightIterator.hasNext())
			return false;
		
		row = rightIterator.next();
		return true;
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
		return rightIterator;
	}

}
