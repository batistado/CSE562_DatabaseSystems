package dubstep;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class Main {
	private static final int BATCH_SIZE = 5;
	private static Map<String, CreateTable> tables = new HashMap<String, CreateTable>();
	static Map<String, TupleSchema> tableSchemas = new HashMap<>();
	
	
	public static void main(String[] args) {
		CCJSqlParser parser;
		
		System.out.println("$> ");
		parser = new CCJSqlParser(System.in);
		Statement queryStatement;
		try {
			while ((queryStatement = parser.Statement()) != null) {
				if (queryStatement instanceof Select) {
					Select selectQuery = (Select) queryStatement;
					evaluateQuery(selectQuery);
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
	
	private static boolean readBatch(BufferedReader reader, String tableName, Expression filter, ArrayList<ArrayList<PrimitiveValue>> filteredRows) throws IOException{
		ArrayList<ArrayList<PrimitiveValue>> unfilteredRows = new ArrayList<ArrayList<PrimitiveValue>>();
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
				if(FilterRows.filterRow(row, filter, tableName)) {
					filteredRows.add(row);
				}
			}
		} else {
			filteredRows.addAll(unfilteredRows);
		}
		
		
		return true;
	}
	
	
	private static ArrayList<ArrayList<PrimitiveValue>> readRowsFromTable(String tableName, Expression filter) {
		ArrayList<ArrayList<PrimitiveValue>> filteredRows = new ArrayList<ArrayList<PrimitiveValue>>();
		String path = "/Users/msyed3/Desktop/data/"+tableName+".csv";
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(path));
			boolean flag = true;
			while(flag) {
				flag = readBatch(br, tableName, filter, filteredRows);	
			}
			br.close();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
		return filteredRows;
	}
		
	public static void createTable(CreateTable table) {
		if (!tables.containsKey(table.getTable().getName())) {
			tables.put(table.getTable().getName(), table);
		}
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
	
	public static void evaluatePlainSelect(PlainSelect plainSelectQuery) {
		FromItem fromItem = plainSelectQuery.getFromItem();
		if (fromItem == null) {
			// Implement expression evaluation
			return;
		}
		else if (fromItem instanceof Select) {
			// Add further steps to process and return rows
			evaluateQuery((Select) fromItem);
		} else {
			evaluateFromTable(plainSelectQuery);
		}
	}
	
	public static void evaluateFromTable(PlainSelect plainSelect) {
		Expression where = plainSelect.getWhere();
		Table fromTable = (Table) plainSelect.getFromItem();
		ArrayList<ArrayList<PrimitiveValue>> filteredRows = readRowsFromTable(fromTable.getName(), where);
		System.out.println(filteredRows.toString());
	}
	
	public static void evaluateQuery(Select selectQuery) {
		if (selectQuery.getSelectBody() instanceof PlainSelect) {
			evaluatePlainSelect((PlainSelect)selectQuery.getSelectBody());
		} else {
			// Write Union logic
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
