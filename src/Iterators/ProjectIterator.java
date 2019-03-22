package Iterators;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import Models.Schema;
import Models.TupleSchema;
import Utils.utils;
import dubstep.Main;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectIterator implements RAIterator{
	private RAIterator rightIterator = null;
	private ArrayList<PrimitiveValue> row;
	private TupleSchema fromSchema;
	private TupleSchema selectSchema;
	private List<SelectItem> selectItems;
	
	public ProjectIterator(RAIterator rightIterator, List<SelectItem> selectItems) {
		this.rightIterator = rightIterator;
		this.selectItems = selectItems;
		setIteratorSchema();
	}
	
	public void resetIterator() {
		rightIterator.resetIterator();
	}
	
	public List<SelectItem> getSelectItems() {
		return this.selectItems;
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		if (!rightIterator.hasNext())
			return false;
		
		row = rightIterator.next();
		return true;
	}

	@Override
	public ArrayList<PrimitiveValue> next() {
		// TODO Auto-generated method stub
		if (selectItems == null || selectItems.isEmpty() || selectItems.get(0) instanceof AllColumns) {
			return row;
		}
		
		ArrayList<PrimitiveValue> resultRow = new ArrayList<>();
		
		for (SelectItem selectItem: selectItems) {
			if (selectItem instanceof AllTableColumns) {
				AllTableColumns t = (AllTableColumns) selectItem;
				Table table = t.getTable();
				
				for (Integer index : fromSchema.getIndexesOfTable(utils.getTableName(table))) {
					resultRow.add(row.get(index));
				}
				continue;
			}
			
			SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
			resultRow.add(utils.filterRowForProjection(row, selectExpressionItem.getExpression(), fromSchema));
		}
		
		return resultRow;
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
		
		addSelectSchema();
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
				if (selectItem instanceof AllTableColumns) {
					columnNumber = addAllTableColumnSchema((AllTableColumns) selectItem, columnNumber);
					continue;
				}
			
				SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
				Expression expression = selectExpressionItem.getExpression();
				String colName, colDatatype;
				if (expression instanceof Column) {
					Column column = (Column) expression;
					colName = utils.getColumnName(selectExpressionItem, column.getWholeColumnName());
					colDatatype = fromSchema.getSchemaByName(column.getWholeColumnName()).getDataType();
					selectSchema.addTuple(colName, columnNumber, colDatatype);
					selectSchema.addTuple(column.getColumnName(), columnNumber, colDatatype);
				} else {
					BinaryExpression binaryExpression = (BinaryExpression) expression;
					colDatatype = utils.getExpressionColumnDatatype(binaryExpression, fromSchema);
					colName = selectExpressionItem.getAlias();
					selectSchema.addTuple(colName, columnNumber, colDatatype);
				}
				columnNumber++;
		}
	}
	
	public Integer addAllTableColumnSchema(AllTableColumns allTableColumns, Integer columnNumber) {
		Table table = allTableColumns.getTable();
		String aliasedTableName = utils.getTableName(table);
		
		Map<String, Schema> schemaByName = Main.tableSchemas.get(table.getName()).schemaByName();
		
		for (String name: schemaByName.keySet()) {
			// Since default columns are referenced as X.A
			String colName = !aliasedTableName.equals(table.getName()) ? aliasedTableName + "." + name.substring(name.lastIndexOf('.') + 1) : name;
			Schema s = Main.tableSchemas.get(table.getName()).getSchemaByName(name);
			selectSchema.addTuple(colName, s.getColumnIndex(), s.getDataType());
			columnNumber++;
		}
		
		return columnNumber;
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
