package Iterators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import Aggregates.*;
import Models.Schema;
import Models.TupleSchema;
import Utils.utils;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class InMemoryGroupByIterator implements RAIterator {
	private RAIterator rightIterator = null;
	private ArrayList<PrimitiveValue> row;
	private TupleSchema fromSchema;
	private TupleSchema selectSchema;
	private List<SelectItem> selectItems;
	private List<Aggregate> aggregates;
	private List<Column> groupByColumns;
	private Integer totalColumns;
	private HashMap<String, ArrayList<Aggregate>> buffer;
	private HashMap<String, ArrayList<PrimitiveValue>> groupByMapping;
	private ArrayList<Function> aggregateFunctions;
	private boolean isFirst = true;

	public InMemoryGroupByIterator(RAIterator rightIterator, List<SelectItem> selectItems) {
		this.rightIterator = rightIterator;
		this.selectItems = selectItems;
		this.buffer = new HashMap<String, ArrayList<Aggregate>>();
		this.setIteratorSchema();
	}
	
	public void pushDownSchema(RAIterator iterator) {
		this.rightIterator = iterator;
		
		setIteratorSchema();
	}

	public void resetIterator() {
		rightIterator.resetIterator();
	}

	private void fillBuffer() {
//		long startTime = System.nanoTime();
		
		while (rightIterator.hasNext()) {
			String hash = "";

			ArrayList<PrimitiveValue> rightRow = rightIterator.next();
			ArrayList<PrimitiveValue> groupByValue = new ArrayList<PrimitiveValue>();

			for (Column groupByColumn : groupByColumns) {
				PrimitiveValue val = utils.projectColumnValue(rightRow, groupByColumn, fromSchema);
				hash += val.toString();
				groupByValue.add(val);
			}

			if (!buffer.containsKey(hash)) {
				ArrayList<Aggregate> currRow = new ArrayList<Aggregate>();
				
				for (int i = 0; i < aggregateFunctions.size(); i++) {
					Function aggregateFunction = aggregateFunctions.get(i);
					Aggregate aggregateObject = aggregates.get(i);
					Aggregate newObject = null;
					Expression functionExpression = null;
					
					if (!aggregateFunction.isAllColumns()) {
						functionExpression = aggregateFunction.getParameters().getExpressions().get(0);
					}
					
					if (aggregateObject instanceof Sum) {
						newObject = new Sum(aggregateObject.getDataType(), aggregateObject.getIndex(),
								functionExpression);
					} else if (aggregateObject instanceof Average) {
						newObject = new Average(aggregateObject.getDataType(), aggregateObject.getIndex(),
								functionExpression);
					} else if (aggregateObject instanceof Min) {
						newObject = new Min(aggregateObject.getDataType(), aggregateObject.getIndex(),
								functionExpression);
					} else if (aggregateObject instanceof Max) {
						newObject = new Max(aggregateObject.getDataType(), aggregateObject.getIndex(),
								functionExpression);
					} else {
						newObject = new Count(aggregateObject.getIndex(), functionExpression);
					}

					if (newObject.getExpression() != null) {
						newObject.addValue(utils.projectColumnValue(rightRow, newObject.getExpression(), fromSchema));
					} else {
						// only for count
						newObject.addValue(new LongValue(0));
					}
					currRow.add(newObject);
				}

				buffer.put(hash, currRow);
				groupByMapping.put(hash, groupByValue);
			} else {
				ArrayList<Aggregate> currRow = buffer.get(hash);

				for (Aggregate aggregate : currRow) {
					if (aggregate.getExpression() != null) {
						aggregate.addValue(utils.projectColumnValue(rightRow, aggregate.getExpression(), fromSchema));
					} else {
						// only for count
						aggregate.addValue(new LongValue(0));
					}
				}
			}
		}
//		long endTime = System.nanoTime();
//
//		long duration = (endTime - startTime);
//		System.out.println("fill buffer time: " + duration * 1/1000000000);
	}

	@Override
	public boolean hasNext() {
		if (isFirst) {
			fillBuffer();
			isFirst = false;
		}
		
		if (this.buffer.size() == 0) {
			row = null;
			return false;
		}
		
		String hashKey = null;
		
		for (String k: this.buffer.keySet()) {
			hashKey = k;
			break;
		}
		
		ArrayList<Aggregate> aggregateRow = this.buffer.get(hashKey);
		ArrayList<PrimitiveValue> groupByValue = this.groupByMapping.get(hashKey);
		
		row = new ArrayList<PrimitiveValue>();
		
		//PrimitiveValue[] tmp = new PrimitiveValue[totalColumns];
		
		for (Aggregate aggregate: aggregateRow) {
			if (aggregate.getIndex() > row.size()) {
				row.add(aggregate.getValue());
			} else {
			row.add(aggregate.getIndex(), aggregate.getValue());
			}
			//tmp[aggregate.getIndex()] = aggregate.getValue();
		}
		
		for (int i = 0; i < groupByColumns.size(); i++) {
			int idx = selectSchema.getSchemaByName(groupByColumns.get(i).getWholeColumnName()).getColumnIndex();
			
			if (idx > row.size()) {
				row.add(groupByValue.get(i));
			} else {
			row.add(idx, groupByValue.get(i));
			}
			//tmp[selectSchema.getSchemaByName(groupByColumns.get(i).getWholeColumnName()).getColumnIndex()] = groupByValue.get(i);
		}
		
		//row.addAll(Arrays.asList(tmp));
		
		this.buffer.remove(hashKey);
		this.groupByMapping.remove(hashKey);
		
		return true;
	}

	public List<SelectItem> getSelectItems() {
		return this.selectItems;
	}

	public void setIteratorSchema() {
		if (rightIterator == null)
			return;

		TupleSchema rightIteratorSchema = rightIterator.getIteratorSchema();
		fromSchema = new TupleSchema();

		Map<String, Schema> schemaByName = rightIteratorSchema.schemaByName();
		for (String name : schemaByName.keySet()) {
			String colName = name;
			Schema s = rightIteratorSchema.getSchemaByName(name);
			fromSchema.addTuple(colName, s.getColumnIndex(), s.getDataType());
		}

		addSelectSchema();

		totalColumns = this.aggregates.size() + this.groupByColumns.size();
	}

	public void addSelectSchema() {
		this.groupByMapping = new HashMap<String, ArrayList<PrimitiveValue>>();
		this.aggregates = new ArrayList<Aggregate>();
		this.groupByColumns = new ArrayList<Column>();
		this.aggregateFunctions = new ArrayList<Function>();
		
		if (selectItems == null || selectItems.isEmpty() || selectItems.get(0) instanceof AllColumns) {
			selectSchema = fromSchema;
			return;
		}

		selectSchema = new TupleSchema();

		// Add aliased columns
		Integer columnNumber = 0;
		for (SelectItem selectItem : selectItems) {
			SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
			Expression expression = selectExpressionItem.getExpression();
			String colName; Integer colDatatype;
			if (expression instanceof Column) {
				Column column = (Column) expression;
				colName = utils.getColumnName(selectExpressionItem, column.getWholeColumnName());
				colDatatype = fromSchema.getSchemaByName(column.getWholeColumnName()).getDataType();
				selectSchema.addTuple(colName, columnNumber, colDatatype);
				selectSchema.addTuple(column.getColumnName(), columnNumber, colDatatype);
				this.groupByColumns.add(column);
			} else {
				// Aggregate functions
				Function aggregateFunction = (Function) expression;
				colName = utils.getColumnName(selectExpressionItem, utils.getFunctionName(aggregateFunction));
				Expression functionExpression = null;

				if (aggregateFunction.isAllColumns()) {
					colDatatype = 2;
				} else {
					functionExpression = aggregateFunction.getParameters().getExpressions().get(0);
					if (functionExpression instanceof Column) {
						Column aggregateColumn = (Column) functionExpression;
						colDatatype = fromSchema.getSchemaByName(aggregateColumn.getWholeColumnName()).getDataType();
					} else if (functionExpression instanceof BinaryExpression) {
						BinaryExpression binaryExpression = (BinaryExpression) functionExpression;
						colDatatype = utils.getExpressionColumnDatatype(binaryExpression, fromSchema);
					} else {
						colDatatype = 2;
					}
				}

				selectSchema.addTuple(colName, columnNumber, colDatatype);
				aggregateFunctions.add(aggregateFunction);

				switch (aggregateFunction.getName()) {
				case "SUM":
					this.aggregates.add(new Sum(colDatatype, columnNumber, functionExpression));
					break;

				case "AVG":
					this.aggregates.add(new Average(colDatatype, columnNumber, functionExpression));
					break;

				case "MIN":
					this.aggregates.add(new Min(colDatatype, columnNumber, functionExpression));
					break;

				case "MAX":
					this.aggregates.add(new Max(colDatatype, columnNumber, functionExpression));
					break;

				case "COUNT":
					this.aggregates.add(new Count(columnNumber, functionExpression));
					break;
				}
			}
			columnNumber++;
		}
	}

	@Override
	public ArrayList<PrimitiveValue> next() {
		// TODO Auto-generated method stub
		return row;
	}

	@Override
	public void resetWhere() {
		// TODO Auto-generated method stub

	}

	@Override
	public TupleSchema getIteratorSchema() {
		// TODO Auto-generated method stub
		return selectSchema;
	}

	@Override
	public void resetProjection() {
		// TODO Auto-generated method stub

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
