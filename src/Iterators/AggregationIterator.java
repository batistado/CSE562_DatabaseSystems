package Iterators;
import java.util.ArrayList;
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

public class AggregationIterator implements RAIterator{
	private RAIterator rightIterator = null;
	private ArrayList<PrimitiveValue> prevRow;
	private ArrayList<PrimitiveValue> row;
	private TupleSchema fromSchema;
	private TupleSchema selectSchema;
	private List<SelectItem> selectItems;
	private List<Aggregate> aggregates;
	private List<Column> groupByColumns;
	private List<PrimitiveValue> groupByValue;
	private Integer totalColumns;
	
	public AggregationIterator (RAIterator rightIterator, List<SelectItem> selectItems) {
		this.rightIterator = rightIterator;
		this.selectItems = selectItems;
		this.aggregates = new ArrayList<Aggregate>();
		this.groupByColumns = new ArrayList<Column>();
		this.groupByValue = new ArrayList<PrimitiveValue>();
		this.setIteratorSchema();
		//System.gc();
	}

	public void resetIterator() {
		rightIterator.resetIterator();
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		if (!rightIterator.hasNext()) {
			return false;
		}
		
		boolean isSameGroup = true;
		
		ArrayList<PrimitiveValue> rightRow = null;
		do {
			rightRow = rightIterator.next();
			
			if (groupByValue.isEmpty()) {
				for (Column groupByColumn : groupByColumns) {
					groupByValue.add(utils.projectColumnValue(rightRow, groupByColumn, fromSchema));
				}
			} else {
				for (int i=0; i < groupByColumns.size(); i++) {
					if (!utils.areEqual(utils.projectColumnValue(rightRow, groupByColumns.get(i), fromSchema), groupByValue.get(i))) {
						isSameGroup = false;
						break;
					}
				}
			}
			
			if (isSameGroup) {
				prevRow = rightRow;
				for (Aggregate aggregate : aggregates) {
					if (aggregate.getExpression() != null) {
						aggregate.addValue(utils.projectColumnValue(rightRow, aggregate.getExpression(), fromSchema));
					} else {
						// only for count
						aggregate.addValue(new LongValue(0));
					}
						
				}
			} else {
				break;
			}
			
			
			if (!rightIterator.hasNext()) {
				if (prevRow != null) {
					PrimitiveValue[] tmp = new PrimitiveValue[totalColumns];
					
					for (Aggregate aggregate: aggregates) {
						tmp[aggregate.getIndex()] = aggregate.getValue();
					}
					
					for (Column groupByColumn : groupByColumns) {
						tmp[selectSchema.getSchemaByName(groupByColumn.getWholeColumnName()).getColumnIndex()] = utils.projectColumnValue(prevRow, groupByColumn, fromSchema);
					}
					
					row = new ArrayList<PrimitiveValue>();
					for(PrimitiveValue val: tmp) {
						row.add(val);
					}
					
					prevRow = null;
					
					return true;
				}
				return false;
			}
			
		} while(isSameGroup);
		
		groupByValue.clear();
		for (Column groupByColumn : groupByColumns) {
			groupByValue.add(utils.projectColumnValue(rightRow, groupByColumn, fromSchema));
		}
		
		PrimitiveValue[] tmp = new PrimitiveValue[totalColumns];
		
		for (Aggregate aggregate: aggregates) {
			tmp[aggregate.getIndex()] = aggregate.getValue();
		}
		
		for (Column groupByColumn : groupByColumns) {
			tmp[selectSchema.getSchemaByName(groupByColumn.getWholeColumnName()).getColumnIndex()] = utils.projectColumnValue(prevRow, groupByColumn, fromSchema);
		}
		
		row = new ArrayList<PrimitiveValue>();
		for(PrimitiveValue val: tmp) {
			row.add(val);
		}
		
		prevRow = rightRow;
		
		for (Aggregate aggregate : aggregates) {
			if (aggregate.getExpression() != null) {
				aggregate.resetAndAddValue(utils.projectColumnValue(rightRow, aggregate.getExpression(), fromSchema));
			} else {
				// only for count
				aggregate.resetAndAddValue(new LongValue(0));
			}
			
		}
		
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
		for (String name: schemaByName.keySet()) {
			String colName = name;
			Schema s = rightIteratorSchema.getSchemaByName(name);
			fromSchema.addTuple(colName, s.getColumnIndex(), s.getDataType());
		}
		
		addSelectSchema();
		
		totalColumns = this.aggregates.size() + this.groupByColumns.size();
	}
	
	public void addSelectSchema() {
		if (selectItems == null || selectItems.isEmpty() || selectItems.get(0) instanceof AllColumns) {
			selectSchema = fromSchema;
			return;
		}
		
		selectSchema = new TupleSchema();
		
		// Add aliased columns
		Integer columnNumber = 0;
		for (SelectItem selectItem: selectItems) {
			SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
			Expression expression = selectExpressionItem.getExpression();
			String colName, colDatatype;
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
					colDatatype = "int";
				} else {
					functionExpression = aggregateFunction.getParameters().getExpressions().get(0);
					if (functionExpression instanceof Column) {
						Column aggregateColumn = (Column) functionExpression;
						colDatatype = fromSchema.getSchemaByName(aggregateColumn.getWholeColumnName()).getDataType();
					} else if (functionExpression instanceof BinaryExpression){
						BinaryExpression binaryExpression = (BinaryExpression) functionExpression;
						colDatatype = utils.getExpressionColumnDatatype(binaryExpression, fromSchema);
					} else {
						colDatatype = "int";
					}
				}
				
				selectSchema.addTuple(colName, columnNumber, colDatatype);
				
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


