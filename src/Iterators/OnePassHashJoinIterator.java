package Iterators;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import Models.Schema;
import Models.TupleSchema;
import Utils.utils;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.statement.select.SelectItem;

public class OnePassHashJoinIterator implements RAIterator{
	private RAIterator leftIterator = null;
	private RAIterator rightIterator = null;
	private ArrayList<PrimitiveValue> row;
	private TupleSchema fromSchema;
	private TupleSchema selectSchema;
	private Expression joinCondition;
	private Map<PrimitiveValue, ArrayList<ArrayList<PrimitiveValue>>> joinMap;
	private LinkedList<ArrayList<PrimitiveValue>> buffer;
	private boolean isFirst = true;
	
	public OnePassHashJoinIterator (RAIterator leftIterator, RAIterator rightIterator, Expression joinCondition) {
		this.joinMap = new HashMap<PrimitiveValue, ArrayList<ArrayList<PrimitiveValue>>>();
		this.buffer = new LinkedList<ArrayList<PrimitiveValue>>();
		this.joinCondition = joinCondition;
		this.leftIterator = leftIterator;
		this.rightIterator = rightIterator;
		this.setIteratorSchema();
	}
	
	public Expression getExpression() {
		return this.joinCondition;
	}
	
	public void pushDownSchema(RAIterator leftIterator, RAIterator rightIterator) {
		this.leftIterator = leftIterator;
		this.rightIterator = rightIterator;
		
		setIteratorSchema();
	}
	
	public void initializeIterator() {
		Expression leftCondition = ((EqualsTo) joinCondition).getLeftExpression();
		while (leftIterator.hasNext()) {
			PrimitiveValue key = utils.projectColumnValue(leftIterator.next(), leftCondition, leftIterator.getIteratorSchema());
			
			ArrayList<ArrayList<PrimitiveValue>> bucket = joinMap.containsKey(key) ? joinMap.get(key): new ArrayList<ArrayList<PrimitiveValue>>();
			bucket.add(leftIterator.next());
			joinMap.put(key, bucket);
		}
	}
	
	@Override
	public void resetWhere() {
	}
	
	@Override
	public void resetProjection() {
	}
	
	public void resetIterator() {
		this.rightIterator.resetIterator();
		buffer.clear();
		row = null;
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		if (isFirst) {
			this.initializeIterator();
			isFirst = false;
		}
		
		if (joinMap.isEmpty())
			return false;
		
		if (!buffer.isEmpty()) {
			row = buffer.pollFirst();
			return true;
		}
		
		Expression rightCondition = ((EqualsTo) joinCondition).getRightExpression();
		PrimitiveValue key;
		while (rightIterator.hasNext()) {
			key = utils.projectColumnValue(rightIterator.next(), rightCondition, rightIterator.getIteratorSchema());
			
			if (joinMap.containsKey(key)) {
				ArrayList<ArrayList<PrimitiveValue>> bucket = joinMap.get(key);
				
				for (ArrayList<PrimitiveValue> leftRow : bucket) {
					ArrayList<PrimitiveValue> mergedRow = new ArrayList<PrimitiveValue>();
					
					mergedRow.addAll(leftRow);
					mergedRow.addAll(rightIterator.next());
						
					buffer.add(mergedRow);
				}
				
				row = buffer.pollFirst();
				
				return true;
			}
		}
		
		return false;
	}

	@Override
	public ArrayList<PrimitiveValue> next() {
		// TODO Auto-generated method stub
		return row;
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
