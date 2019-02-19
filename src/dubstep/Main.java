package dubstep;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

public class Main {
	private static final int BATCH_SIZE = 5;
	static Map<String, TupleSchema> tableSchemas = new HashMap<>();
	private static String currentTableName;
	
	
	public static void main(String[] args) {
		CCJSqlParser parser;
		
		System.out.println("$> ");
		parser = new CCJSqlParser(System.in);
		Statement queryStatement;
		try {
			while ((queryStatement = parser.Statement()) != null) {
				if (queryStatement instanceof Select) {
					Select selectQuery = (Select) queryStatement;
					System.out.println(evaluateQuery(selectQuery).toString());
				}
				else if (queryStatement instanceof CreateTable) {
					createTable((CreateTable) queryStatement);
				}
				System.out.println("$> ");
				parser = new CCJSqlParser(System.in);
			}
		} catch (ParseException e) {
			System.out.println("Can't read query" + args[0]);
			e.printStackTrace();
		}
	}
	
	public static void aliasSchema(String tableName, String aliasName) {
		if (tableSchemas.containsKey(tableName)) {
			TupleSchema ts = new TupleSchema();
			Map<String, Schema> schemaByName = tableSchemas.get(tableName).schemaByName();
			
			for (String name: schemaByName.keySet()) {
				String colName = aliasName + "." + name.substring(name.lastIndexOf('.') + 1);
				Schema s = tableSchemas.get(tableName).getSchemaByName(name);
				ts.addTuple(colName, s.getColumnIndex(), s.getDataType());
			}
			
			tableSchemas.put(aliasName, ts);
		}
	}
	
	private static boolean readBatch(BufferedReader reader, Table table, Expression filter, ArrayList<ArrayList<PrimitiveValue>> filteredRows) throws IOException{
		ArrayList<ArrayList<PrimitiveValue>> unfilteredRows = new ArrayList<ArrayList<PrimitiveValue>>();
		String tableName = table.getName();
		currentTableName = tableName;
		
		if (table.getAlias() != null) {
			aliasSchema(tableName, table.getAlias());
			currentTableName = table.getAlias();
		}
		
		for(int i = 0; i < BATCH_SIZE; i++) {
			String line = reader.readLine();
			if(line != null){
				String[] row = line.split("\\|");
				int j = 0;
				ArrayList<PrimitiveValue> tmp = new ArrayList<PrimitiveValue>();
				for(String x : row) {
					String colDatatype = tableSchemas.get(tableName).getSchemaByIndex(j).getDataType();
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
				unfilteredRows.add(tmp);
				
			}else {
				return false;
			}
		}
		
		if (filter != null) {
			for(ArrayList<PrimitiveValue> row : unfilteredRows) {
				if(FilterRows.filterRow(row, filter, table.getAlias() != null ? table.getAlias() : tableName)) {
					filteredRows.add(row);
				}
			}
		} else {
			filteredRows.addAll(unfilteredRows);
		}
		
		
		return true;
	}
	
	
	private static ArrayList<ArrayList<PrimitiveValue>> readRowsFromTable(Table table, Expression filter) {
		ArrayList<ArrayList<PrimitiveValue>> filteredRows = new ArrayList<ArrayList<PrimitiveValue>>();
		String path = "/Users/msyed3/Downloads/sample queries/NBA_Examples/" + table.getName() + ".dat";
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(path));
			boolean flag = true;
			while(flag) {
				flag = readBatch(br, table, filter, filteredRows);	
			}
			br.close();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
		return filteredRows;
	}
		
	public static void createTable(CreateTable table) {
		if (!tableSchemas.containsKey(table.getTable().getName())) {
			TupleSchema ts = new TupleSchema();
			Integer i = 0;
			for (ColumnDefinition columnDefinition : table.getColumnDefinitions()) {
				ts.addTuple(table.getTable().getName() + "." + columnDefinition.getColumnName(), i, columnDefinition.getColDataType().getDataType());
				i++;
			}
			tableSchemas.put(table.getTable().getName(), ts);
		}
	}
	
	public static ArrayList<ArrayList<PrimitiveValue>> applyProjection(ArrayList<ArrayList<PrimitiveValue>> rows, List<SelectItem> selectItems) {
		if (selectItems.get(0) instanceof AllColumns) {
			return rows;
		}
		
		ArrayList<ArrayList<PrimitiveValue>> resultRows = new ArrayList<>();
		
		for (ArrayList<PrimitiveValue> row: rows) {
			ArrayList<PrimitiveValue> resultRow = new ArrayList<PrimitiveValue>();
			for (SelectItem selectItem: selectItems) {
					SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
					resultRow.add(FilterRows.filterRowForProjection(row, selectExpressionItem.getExpression(), currentTableName));
			}
			resultRows.add(resultRow);
		}
		
		String projectionTableName = "Projection." + currentTableName;

		// Update column numbers
		Integer columnNumber = 0;
		for (SelectItem selectItem: selectItems) {
				SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
				Expression expression = selectExpressionItem.getExpression();
				Column column = (Column) expression;
				addColumnAlias(column.getWholeColumnName(), selectExpressionItem.getAlias(), columnNumber, projectionTableName);
				columnNumber++;
		}
		
		currentTableName = projectionTableName;
		
		return resultRows;
	}
	
	public static void addColumnAlias(String columnName, String aliasName, Integer columnNumber, String projectionTableName) {		
		TupleSchema ts = tableSchemas.containsKey(projectionTableName) ? tableSchemas.get(projectionTableName) : new TupleSchema();
		
		String fullColumnName = columnName.startsWith(currentTableName) ? columnName : currentTableName + "." + columnName;
		Schema s = tableSchemas.get(currentTableName).getSchemaByName(fullColumnName);
		
		if (aliasName != null) {
			String colName = projectionTableName + "." + aliasName;
			ts.addTuple(colName, columnNumber, s.getDataType());
		} else {
			String colName = projectionTableName + "." + columnName;
			ts.addTuple(colName, columnNumber, s.getDataType());
		}
		
		tableSchemas.put(projectionTableName, ts);
	}
	
	public static ArrayList<ArrayList<PrimitiveValue>> evaluatePlainSelect(PlainSelect plainSelectQuery) {
		FromItem fromItem = plainSelectQuery.getFromItem();
		
		if (fromItem == null) {
			// Implement expression evaluation
			return null;
		}
		else if (fromItem instanceof SubSelect) {
			// Add further steps to process and return rows
			return applyProjection(evaluateSubQuery((SubSelect) fromItem), plainSelectQuery.getSelectItems());
		} else {
			return applyProjection(evaluateFromTables(plainSelectQuery), plainSelectQuery.getSelectItems());
		}
	}
	
	public static ArrayList<ArrayList<PrimitiveValue>> evaluateSubQuery(SubSelect subQuery) {
		if (subQuery.getSelectBody() instanceof PlainSelect) {
			return evaluatePlainSelect((PlainSelect)subQuery.getSelectBody());
		} else {
			// Write Union logic
			return evaluateUnion((Union) subQuery.getSelectBody());
		}
	}
	
	public static ArrayList<ArrayList<PrimitiveValue>> evaluateUnion(Union union){
		// Reverse the logic for union and union all
		if (union.isAll()) {
			// union all logic
			return null;
		}
		
		ArrayList<ArrayList<PrimitiveValue>> result = new ArrayList<>();
		
		for (PlainSelect plainSelect : union.getPlainSelects()) {
			result.addAll(evaluatePlainSelect(plainSelect));
		}
		
		return result;
	}
	
	public static ArrayList<ArrayList<PrimitiveValue>> evaluateFromTables(PlainSelect plainSelect) {
		Expression where = plainSelect.getWhere();
		Table fromTable = (Table) plainSelect.getFromItem();
		List<Join> joins = plainSelect.getJoins();
		if (joins == null || joins.isEmpty()) {
			return evaluateFromTable(fromTable, where);
		}
		
		return evaluateJoins(fromTable, joins, where);
	}
	
	public static ArrayList<ArrayList<PrimitiveValue>> filterResults(ArrayList<ArrayList<PrimitiveValue>> unfilteredRows, Expression filter, String tableName) {
		ArrayList<ArrayList<PrimitiveValue>> filteredRows = new ArrayList<ArrayList<PrimitiveValue>>();
		if (filter != null) {
			for(ArrayList<PrimitiveValue> row : unfilteredRows) {
				if(FilterRows.filterRow(row, filter, tableName)) {
					filteredRows.add(row);
				}
			}
		} else {
			filteredRows.addAll(unfilteredRows);
		}
		
		return filteredRows;
	}
	
	public static ArrayList<ArrayList<PrimitiveValue>> evaluateJoins(Table fromTable, List<Join> joins, Expression filter) {
		ArrayList<ArrayList<PrimitiveValue>> result = readRowsFromTable(fromTable, null);
		String joinName = fromTable.getAlias() == null ? fromTable.getName() : fromTable.getAlias();
		for (Join join: joins) {
			Table rightTable = (Table) join.getRightItem();
			ArrayList<ArrayList<PrimitiveValue>> rightResult = readRowsFromTable(rightTable, null);
			joinName = crossJoin(result, rightResult, joinName, rightTable, join.getOnExpression());
		}
		
		currentTableName = joinName;
		
		return filterResults(result, filter, joinName);
	}
	
	public static String crossJoin(ArrayList<ArrayList<PrimitiveValue>> leftRows, ArrayList<ArrayList<PrimitiveValue>> rightRows, String leftTableName, Table rightTable, Expression joinExpression) {
		ArrayList<ArrayList<PrimitiveValue>> result = new ArrayList<ArrayList<PrimitiveValue>>();
		for (ArrayList<PrimitiveValue> leftRow : leftRows) {
			for (ArrayList<PrimitiveValue> rightRow : rightRows) {
				ArrayList<PrimitiveValue> tmpRow = new ArrayList<>();
				tmpRow.addAll(leftRow);
				tmpRow.addAll(rightRow);
				result.add(tmpRow);
			}
		}
		
		String joinName = mergeSchemas(leftTableName, rightTable);
		
		if (joinExpression != null) {
			result = filterResults(result, joinExpression, joinName);
		}
		
		leftRows.clear();
		leftRows.addAll(result);
		
		return joinName;
	}
	
	public static String getColumnName(String prefixName, String name, Integer stripAt) {
		int count = 0, begIndex = 0;
		for (int i=0; i < name.length(); i++) {
			if (name.charAt(i) == '.')
				count++;
			
			if (count == stripAt) {
				begIndex = i;
			}
			
			if (count > stripAt)
				break;
		}
		
		if (count > stripAt) {
			return prefixName + "." + name.substring(begIndex);
		}
		
		return prefixName + "." + name;
	}
	
	public static String mergeSchemas(String leftTableName, Table rightTable) {
		String rightTableName = rightTable.getAlias() == null ? rightTable.getName() : rightTable.getAlias();
		
		String joinName = leftTableName + 'X' + rightTableName;
		if (!tableSchemas.containsKey(joinName)) {
			TupleSchema ts = new TupleSchema();
			Map<String, Schema> schemaByName = tableSchemas.get(leftTableName).schemaByName();
			
			Integer maxIndex = -1;
			for (String name: schemaByName.keySet()) {
				String colName = getColumnName(joinName, name, 1);
				Schema s = tableSchemas.get(leftTableName).getSchemaByName(name);
				ts.addTuple(colName, s.getColumnIndex(), s.getDataType());
				
				if (s.getColumnIndex() > maxIndex) {
					maxIndex = s.getColumnIndex();
				}
			}
			
			schemaByName = tableSchemas.get(rightTableName).schemaByName();
			
			for (String name: schemaByName.keySet()) {
				String colName = getColumnName(joinName, name, 1);
				Schema s = tableSchemas.get(rightTableName).getSchemaByName(name);
				ts.addTuple(colName, s.getColumnIndex() + maxIndex + 1, s.getDataType());
			}
			
			tableSchemas.put(joinName, ts);
		}
		
		return joinName;
	}
	
	public static ArrayList<ArrayList<PrimitiveValue>> evaluateFromTable(Table fromTable, Expression where) {
		return readRowsFromTable(fromTable, where);
	}
	
	public static ArrayList<ArrayList<PrimitiveValue>> evaluateQuery(Select selectQuery) {
		if (selectQuery.getSelectBody() instanceof PlainSelect) {
			return evaluatePlainSelect((PlainSelect)selectQuery.getSelectBody());
		} else {
			return evaluateUnion((Union) selectQuery.getSelectBody());
		}
	}
	
	public static void printer(String tableName) {
		String path = "/Users/Kamran/data/" + tableName + ".csv";
		String line;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(path));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		try {
			while ((line = br.readLine()) != null) {
				System.out.println(line);
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
