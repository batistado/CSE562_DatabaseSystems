package Iterators;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import Models.Schema;
import Models.TupleSchema;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.SelectItem;

public class CrossProductIterator implements RAIterator{
	private RAIterator leftIterator = null;
	private RAIterator rightIterator = null;
	private ArrayList<PrimitiveValue> leftRow;
	private ArrayList<PrimitiveValue> rightRow;
	private TupleSchema fromSchema;
	private TupleSchema selectSchema;
	
	public CrossProductIterator (RAIterator leftIterator, RAIterator rightIterator) {
		this.leftIterator = leftIterator;
		this.rightIterator = rightIterator;
		setIteratorSchema();
	}
	
	@Override
	public void resetWhere() {
	}
	
	@Override
	public void resetProjection() {
	}
	
	public void resetIterator() {
		this.leftIterator.resetIterator();
		this.rightIterator.resetIterator();
		leftRow = null;
		rightRow = null;
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub	
		if (leftRow == null && !leftIterator.hasNext())
			return false;

		if (!rightIterator.hasNext()) {
			rightIterator.resetIterator();
			
			if (!rightIterator.hasNext())
				return false;
			
			if (!leftIterator.hasNext())
				return false;
		}
		
		
		
		rightRow = rightIterator.next();
		leftRow = leftIterator.next();
		
		return true;
	}

	@Override
	public ArrayList<PrimitiveValue> next() {
		// TODO Auto-generated method stub
		ArrayList<PrimitiveValue> mergedRow = new ArrayList<>();
		
		mergedRow.addAll(leftRow);
		mergedRow.addAll(rightRow);
		
		return mergedRow;
	}
	
	
	
	public TupleSchema getIteratorSchema() {
		return fromSchema;
	}
	
	public TupleSchema getSelectSchema() {
		return selectSchema;
	}
	
	public void setIteratorSchema() {
		TupleSchema leftIteratorSchema = leftIterator.getIteratorSchema();
		TupleSchema rightIteratorSchema = rightIterator.getIteratorSchema();
		fromSchema = new TupleSchema();
		
		Map<String, Schema> schemaByName = leftIteratorSchema.schemaByName();
		Integer maxIndex = -1;
		
		for (String name: schemaByName.keySet()) {
			String colName = name;
			Schema s = leftIteratorSchema.getSchemaByName(name);
			fromSchema.addTuple(colName, s.getColumnIndex(), s.getDataType());
			
			if (s.getColumnIndex() > maxIndex) {
				maxIndex = s.getColumnIndex();
			}
		}
		
		schemaByName = rightIteratorSchema.schemaByName();
		for (String name: schemaByName.keySet()) {
			String colName = name;
			Schema s = rightIteratorSchema.getSchemaByName(name);
			fromSchema.addTuple(colName, s.getColumnIndex() + maxIndex + 1, s.getDataType());
		}
	}

	@Override
	public void addSelectItems(List<SelectItem> selectItems) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public RAIterator getLeftIterator() {
		// TODO Auto-generated method stub
		return leftIterator;
	}

	@Override
	public RAIterator getRightIterator() {
		// TODO Auto-generated method stub
		return rightIterator;
	}
}

