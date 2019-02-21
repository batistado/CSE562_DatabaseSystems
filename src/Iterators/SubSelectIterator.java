package Iterators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import Utils.utils;
import dubstep.Schema;
import dubstep.TupleSchema;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class SubSelectIterator implements RAIterator {
	private RAIterator leftIterator = null;
	private ArrayList<PrimitiveValue> row;
	private List<SelectItem> selectItems;
	private TupleSchema selectSchema;
	private TupleSchema fromSchema;
	private Expression where;
	
	public SubSelectIterator(RAIterator leftIterator, List<SelectItem> selectItems, Expression where) {
		this.leftIterator = leftIterator;
		this.selectItems = selectItems;
		this.where = where;
		setIteratorSchema();
	}
	
	public void setIteratorSchema() {
		TupleSchema leftIteratorSchema = leftIterator.getSelectSchema();
		fromSchema = new TupleSchema();
		selectSchema = new TupleSchema();
		
		// Copy schema
		Map<String, Schema> schemaByName = leftIteratorSchema.schemaByName();
		
		for (String name: schemaByName.keySet()) {
			String colName = name;
			Schema s = leftIteratorSchema.getSchemaByName(name);
			fromSchema.addTuple(colName, s.getColumnIndex(), s.getDataType());
		}
		
		// Don't update schema if select *
		if (selectItems == null || selectItems.isEmpty() || selectItems.get(0) instanceof AllColumns) {
			return;
		}
		
		// Update Schema
		Integer columnNumber = 0;
		for (SelectItem selectItem: selectItems) {
				SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
				Expression expression = selectExpressionItem.getExpression();
				Column column = (Column) expression;
				String colDatatype = fromSchema.getSchemaByName(column.getWholeColumnName()).getDataType();
				String colName = utils.getColumnName(selectExpressionItem, column.getWholeColumnName());
				selectSchema.addTuple(colName, columnNumber, colDatatype);
				selectSchema.addTuple(column.getColumnName(), columnNumber, colDatatype);
				columnNumber++;
		}
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
		
	}

	@Override
	public void resetProjection() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ArrayList<PrimitiveValue> next() {
		// TODO Auto-generated method stub
		return projectedRow();
	}

	@Override
	public boolean hasNext() {
		if (!leftIterator.hasNext())
			return false;
		
		
		do {
			row = leftIterator.next();
			if (where == null || utils.filterRow(row, where, fromSchema)) {
				return true;
			}
		} while (leftIterator.hasNext());
		
		return false;
	}
	
	public ArrayList<PrimitiveValue> projectedRow() {
		if (selectItems == null || selectItems.isEmpty() || selectItems.get(0) instanceof AllColumns) {
			return row;
		}
		
		ArrayList<PrimitiveValue> resultRow = new ArrayList<>();
		
		for (SelectItem selectItem: selectItems) {
			SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
			resultRow.add(utils.filterRowForProjection(row, selectExpressionItem.getExpression(), fromSchema));
		}
		
		return resultRow;
	}

	@Override
	public void addSelectItems(List<SelectItem> selectItems) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public TupleSchema getSelectSchema() {
		// TODO Auto-generated method stub
		return selectSchema;
	}

}
