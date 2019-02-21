package Iterators;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import Utils.utils;
import dubstep.FilterRows;
import dubstep.Main;
import dubstep.Schema;
import dubstep.TupleSchema;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class FromIterator implements RAIterator{
	private RAIterator leftIterator = null;
	private BufferedReader reader;
	private ArrayList<PrimitiveValue> row;
	private String line;
	private Table table;
	private Expression where;
	private TupleSchema fromSchema;
	private TupleSchema selectSchema;
	private List<SelectItem> selectItems;
	
	public FromIterator (Table table, Expression where, List<SelectItem> selectItems) {
		this.table = table;
		this.where = where;
		this.selectItems = selectItems;
		initializeReader();
		setIteratorSchema();
	}
	
	public FromIterator (RAIterator leftIterator, Table table, Expression where, List<SelectItem> selectItems) {
		this.leftIterator = leftIterator;
		this.table = table;
		this.where = where;
		this.selectItems = selectItems;
		this.leftIterator.resetWhere();
		this.leftIterator.resetProjection();
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
			reader = new BufferedReader(new FileReader(DIR + table.getName() + ".dat"));
			
			if (leftIterator != null)
				line = reader.readLine();
			
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		try {
			
			if (leftIterator == null) {
				while ((line = reader.readLine()) != null) {
					row = getLeftRow();
					if (where == null || utils.filterRow(row, where, fromSchema))
						return true;
				}
				
				return false;
			}
			
			if (!leftIterator.hasNext()) {
				leftIterator.resetIterator();
				line = reader.readLine();
			}
			
			if (line == null)
				return false;
				
			
			do {
				do {
					ArrayList<PrimitiveValue> tmp = new ArrayList<>();
					row = getLeftRow();
					
					tmp.addAll(leftIterator.next());
					tmp.addAll(row);
					
					if (where == null || utils.filterRow(tmp, where, fromSchema)) {
						row = tmp;
						return true;
					}
				} while (leftIterator.hasNext());
				
				leftIterator.resetIterator();
				
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
		
		if (leftIterator == null) {
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
			TupleSchema leftIteratorSchema = leftIterator.getIteratorSchema();
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
			
			schemaByName = Main.tableSchemas.get(table.getName()).schemaByName();
			
			for (String name: schemaByName.keySet()) {
				// Since default columns are referenced as X.A
				String colName = !aliasedTableName.equals(table.getName()) ? aliasedTableName + "." + name.substring(name.lastIndexOf('.') + 1) : name;
				Schema s = Main.tableSchemas.get(table.getName()).getSchemaByName(name);
				fromSchema.addTuple(colName, s.getColumnIndex() + maxIndex + 1, s.getDataType());
			}
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
}
