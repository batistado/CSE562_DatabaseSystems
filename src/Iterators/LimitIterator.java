package Iterators;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import Models.Schema;
import Models.TupleSchema;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.SelectItem;

public class LimitIterator implements RAIterator{
	private RAIterator rightIterator = null;
	private ArrayList<PrimitiveValue> row;
	private TupleSchema fromSchema;
	private TupleSchema selectSchema;
	private Limit limit;
	private Long currentCount = (long) 0;
	
	public LimitIterator(RAIterator rightIterator, Limit limit) {
		this.rightIterator = rightIterator;
		this.limit = limit;
		setIteratorSchema();
	}
	
	public Limit getLimit() {
		return this.limit;
	}
	
	public void resetIterator() {
		rightIterator.resetIterator();
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		if (!rightIterator.hasNext() || this.currentCount >= this.limit.getRowCount())
			return false;
		
		this.currentCount++;
		row = rightIterator.next();
		return true;
	}

	@Override
	public ArrayList<PrimitiveValue> next() {
		// TODO Auto-generated method stub
		return row;
	}
	
	public TupleSchema getIteratorSchema() {
		return selectSchema;
	}
	
	public void setIteratorSchema() {
		if (rightIterator == null)
			return;
		
		TupleSchema rightIteratorSchema = rightIterator.getIteratorSchema();
		fromSchema = new TupleSchema();
		
		Map<String, Schema> schemaByName = rightIteratorSchema.schemaByName();
		for (String name: schemaByName.keySet()) {
			String colName = name;
			Schema s = rightIteratorSchema.getSchemaByName(name);
			fromSchema.addTuple(colName, s.getColumnIndex(), s.getDataType());
		}
	}


	@Override
	public void resetWhere() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void resetProjection() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public TupleSchema getSelectSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addSelectItems(List<SelectItem> selectItems) {
		// TODO Auto-generated method stub
		
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
