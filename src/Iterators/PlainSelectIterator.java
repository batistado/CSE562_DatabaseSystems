package Iterators;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import Models.Schema;
import Models.TupleSchema;
import Utils.utils;
import dubstep.Main;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class PlainSelectIterator implements RAIterator{
	private RAIterator rightIterator = null;
	private BufferedReader reader;
	private ArrayList<PrimitiveValue> row;
	private String line;
	private Table table;
	private Expression where;
	private Expression joinOn;
	private TupleSchema fromSchema;
	private TupleSchema selectSchema;
	private List<SelectItem> selectItems;
	
	public PlainSelectIterator (Table table, Expression where, List<SelectItem> selectItems) {
		this.table = table;
		this.where = where;
		this.selectItems = selectItems;
		initializeReader();
		setIteratorSchema();
	}
	
	public PlainSelectIterator (RAIterator rightIterator, Table table, Expression where, Expression joinOn, List<SelectItem> selectItems) {
		this.rightIterator = rightIterator;
		this.table = table;
		this.where = where;
		this.selectItems = selectItems;
		this.joinOn = joinOn;
		this.rightIterator.resetWhere();
		this.rightIterator.resetProjection();
		initializeReader();
		setIteratorSchema();
	}
	
	
	public void addSelectItems(List<SelectItem> selectItems) {
		this.selectItems = selectItems;
		addSelectSchema();
	}
	
	@Override
	public void resetWhere() {
		this.where = null;
	}
	
	@Override
	public void resetProjection() {
		this.selectItems = null;
	}
	
	public void resetIterator() {
		try {
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		initializeReader();
	}
	
	public void initializeReader() {
		try {
			reader = new BufferedReader(new FileReader(DIR + table.getName() + ".csv"));
			
			if (rightIterator != null)
				line = reader.readLine();
			
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		try {
			if (rightIterator == null) {
				while ((line = reader.readLine()) != null) {
					row = getLeftRow();
					if (where == null || utils.filterRow(row, where, fromSchema))
						return true;
				}
				
				return false;
			}
			
			if (!rightIterator.hasNext()) {
				rightIterator.resetIterator();
				line = reader.readLine();
			}
			
			if (line == null)
				return false;
				
			
			do {
				do {
					ArrayList<PrimitiveValue> tmp = new ArrayList<>();
					row = getLeftRow();
					
					tmp.addAll(row);
					tmp.addAll(rightIterator.next());
					
					if (where == null && joinOn == null) {
						row = tmp;
						return true;
					} else if (where != null && joinOn != null) {
						if (utils.filterRow(tmp, where, fromSchema) && utils.filterRow(tmp, joinOn, fromSchema)) {
							row = tmp;
							return true;
						}	
					} else {
						Expression filter = where == null ? joinOn : where;
						if (utils.filterRow(tmp, filter, fromSchema)) {
							row = tmp;
							return true;
						}
					}
				} while (rightIterator.hasNext());
				
				rightIterator.resetIterator();
				
			} while ((line = reader.readLine()) != null);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		return false;
	}
	
	public ArrayList<PrimitiveValue> getLeftRow(){
		String[] row = line.split("\\|");
		int j = 0;
		ArrayList<PrimitiveValue> tmp = new ArrayList<PrimitiveValue>();
		for(String x : row) {
			String colDatatype = Main.tableSchemas.get(table.getName()).getSchemaByIndex(j).getDataType();
			if(colDatatype.equals("string") || colDatatype.equals("varchar") || colDatatype.equals("char")) {
				StringValue val = new StringValue(x);
				tmp.add(val);
				j++;
			}
			else if(colDatatype.equals("int")){
				LongValue val = new LongValue(x);
				tmp.add(val);
				j++;
			}
			else if(colDatatype.equals("decimal")) {
				DoubleValue val = new DoubleValue(x);
				tmp.add(val);
				j++;
			}
			else if(colDatatype.equals("date")){
				DateValue val = new DateValue(x);
				tmp.add(val);
				j++;
			}
			
		}
		
		return tmp;
	}

	@Override
	public ArrayList<PrimitiveValue> next() {
		// TODO Auto-generated method stub
		return projectedRow();
	}
	
	public TupleSchema getIteratorSchema() {
		return fromSchema;
	}
	
	public TupleSchema getSelectSchema() {
		return selectSchema;
	}
	
	public void setIteratorSchema() {
		String aliasedTableName = utils.getTableName(table);
		
		if (rightIterator == null) {
			if (!aliasedTableName.equals(table.getName())) {
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
		} else {
			TupleSchema rightIteratorSchema = rightIterator.getIteratorSchema();
			fromSchema = new TupleSchema();
			
			Map<String, Schema> schemaByName;
			schemaByName = Main.tableSchemas.get(table.getName()).schemaByName();
			Integer maxIndex = -1;
			
			for (String name: schemaByName.keySet()) {
				// Since default columns are referenced as X.A
				String colName = !aliasedTableName.equals(table.getName()) ? aliasedTableName + "." + name.substring(name.lastIndexOf('.') + 1) : name;
				Schema s = Main.tableSchemas.get(table.getName()).getSchemaByName(name);
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
		
		if (!aliasedTableName.equals(table.getName())) {
			// Add aliased schema to main tables schema. Helps for All Table Columns
			Main.tableSchemas.put(aliasedTableName, fromSchema);
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
	
	public ArrayList<PrimitiveValue> projectedRow() {
		if (selectItems == null || selectItems.isEmpty() || selectItems.get(0) instanceof AllColumns) {
			return row;
		}
		
		ArrayList<PrimitiveValue> resultRow = new ArrayList<>();
		
		for (SelectItem selectItem: selectItems) {
			if (selectItem instanceof AllTableColumns) {
				resultRow.addAll(row);
				continue;
			}
			
			SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
			resultRow.add(utils.filterRowForProjection(row, selectExpressionItem.getExpression(), fromSchema));
		}
		
		return resultRow;
	}
}
