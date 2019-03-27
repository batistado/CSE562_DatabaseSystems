package Iterators;
import java.io.BufferedReader;
import java.io.*;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import Models.Schema;
import Models.TupleSchema;
import Utils.Sort;
import Utils.utils;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectItem;

public class SortMergeJoinIterator implements RAIterator{
	private RAIterator leftIterator = null;
	private RAIterator rightIterator = null;
	private ArrayList<PrimitiveValue> row;
	private TupleSchema fromSchema;
	private TupleSchema selectSchema;
	private Expression joinCondition;
	private String leftFileName;
	private String rightFileName;
	private boolean isInverted;
	private ArrayList<ArrayList<PrimitiveValue>> buffer;
	private BufferedReader leftReader;
	private BufferedReader rightReader;
	private String leftLine;
	private String rightLine;
	
	public SortMergeJoinIterator (RAIterator leftIterator, RAIterator rightIterator, Expression joinCondition) {
		this.buffer = new ArrayList<ArrayList<PrimitiveValue>>();
		this.joinCondition = joinCondition;
		this.leftIterator = leftIterator;
		this.rightIterator = rightIterator;
		isInverted = false;
		this.setIteratorSchema();
		this.initializeIterator();
		this.initializeReader();
		System.gc();
	}
	
	private void initializeReader() {
		// TODO Auto-generated method stub
		try {
			leftReader = new BufferedReader(new FileReader(leftFileName));
			rightReader = new BufferedReader(new FileReader(rightFileName));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Expression getExpression() {
		return this.joinCondition;
	}
	
	public void initializeIterator() {
		EqualsTo equalsToExpression = (EqualsTo) joinCondition;
		
		isInverted = leftIterator.getIteratorSchema()
				.containsKey(utils.getColumnName((Column) equalsToExpression.getLeftExpression())) ? false : true;
		
		
		if (!isInverted) {
			OrderByElement order = new OrderByElement();
			order.setExpression((Column) equalsToExpression.getLeftExpression());
			order.setAsc(true);
			List<OrderByElement> orderByElements = new ArrayList<OrderByElement>();
			orderByElements.add(order);
			leftFileName = new Sort(leftIterator, orderByElements, leftIterator.getIteratorSchema(), DIR, buffer).sortData();
			
			
			order.setExpression((Column) equalsToExpression.getRightExpression());
			order.setAsc(true);
			orderByElements.clear();
			orderByElements.add(order);
			rightFileName = new Sort(rightIterator, orderByElements, rightIterator.getIteratorSchema(), DIR, buffer).sortData();
			
			return;
		}
		
		OrderByElement order = new OrderByElement();
		order.setExpression((Column) equalsToExpression.getRightExpression());
		order.setAsc(true);
		List<OrderByElement> orderByElements = new ArrayList<OrderByElement>();
		orderByElements.add(order);
		leftFileName = new Sort(leftIterator, orderByElements, leftIterator.getIteratorSchema(), DIR, buffer).sortData();
		
		
		order.setExpression((Column) equalsToExpression.getLeftExpression());
		order.setAsc(true);
		orderByElements.clear();
		orderByElements.add(order);
		rightFileName = new Sort(rightIterator, orderByElements, rightIterator.getIteratorSchema(), DIR, buffer).sortData();
		
		return;
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
	
	private ArrayList<PrimitiveValue> getRow(boolean isLeftLine) {
		// TODO Auto-generated method stub
		String line = null;
		TupleSchema schema = null;
		
		if (isLeftLine) {
			line = leftLine;
			schema = leftIterator.getIteratorSchema();
		} else {
			line = rightLine;
			schema = rightIterator.getIteratorSchema();
		}
		
		String[] row = line.split("\\|");
		int j = 0;
		ArrayList<PrimitiveValue> tmp = new ArrayList<PrimitiveValue>();
		for(String x : row) {
			String colDatatype = schema.getSchemaByIndex(j).getDataType();
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

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		try {
		if (leftReader == null || rightReader == null || (leftLine = leftReader.readLine()) == null || (rightLine = rightReader.readLine()) == null){
			if (leftReader != null) {
				leftReader.close();
				leftReader = null;
			}
			
			if (rightReader != null) {
				rightReader.close();
				rightReader = null;
			}
			
			row = null;
			return false;
		}
		
		
		row = new ArrayList<PrimitiveValue>();
		while (true) {
			row.clear();
			ArrayList<PrimitiveValue> leftRow = getRow(true);
			ArrayList<PrimitiveValue> rightRow = getRow(false);
			
			row.addAll(leftRow);
			row.addAll(rightRow);
			
			if (utils.filterRow(row, joinCondition, fromSchema)) {
				return true;
			}
			
			EqualsTo equalsToExpression = (EqualsTo) joinCondition;
			Expression e = new GreaterThan(equalsToExpression.getLeftExpression(), equalsToExpression.getRightExpression());
			
			if (utils.filterRow(row, e, fromSchema)) {
				if (!isInverted) {
					if ((rightLine = rightReader.readLine()) == null) {
						rightReader.close();
						return false;
					}
				} else {
					if ((leftLine = leftReader.readLine()) == null) {
						leftReader.close();
						return false;
					}
				}
			} else {
				if (!isInverted) {
					if ((leftLine = leftReader.readLine()) == null) {
						leftReader.close();
						return false;
					}
					
				} else {
					if ((rightLine = rightReader.readLine()) == null) {
						rightReader.close();
						return false;
					}
				}
			}
		}
		} catch (IOException e) {
			e.printStackTrace();
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

