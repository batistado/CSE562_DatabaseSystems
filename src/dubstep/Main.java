package dubstep;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class Main {
	private static Expression exp;
	private static final int BATCH_SIZE = 5;
	private static Map<String, CreateTable> tables = new HashMap<String, CreateTable>();
	static Map<String, Integer> schema = new HashMap<String, Integer>();
	private static ArrayList<ArrayList<PrimitiveValue>> filteredRows;
	public static void main(String[] args) {
		CCJSqlParser parser;
		
		System.out.println("$> ");
		parser = new CCJSqlParser(System.in);
		Statement queryStatement;
		try {
			while ((queryStatement = parser.Statement()) != null) {
				filteredRows = new ArrayList<ArrayList<PrimitiveValue>>();
				if (queryStatement instanceof Select) {
					Select q = (Select) queryStatement;
					if(q.getSelectBody() instanceof PlainSelect) {
						PlainSelect query = (PlainSelect) q.getSelectBody();
						exp = query.getWhere();
						System.out.println(exp.toString());
						readCSV("r");
						System.out.println(filteredRows);
					}
				}
				else if (queryStatement instanceof CreateTable) {
					createTable((CreateTable) queryStatement);
					createSchema((CreateTable) queryStatement);
				}
				System.out.println("$> ");
				parser = new CCJSqlParser(System.in);
			}
		} catch (ParseException e) {
			System.out.println("Can't read query" + args[0]);
			e.printStackTrace();
		}

	}
	
	
	private static String[] typeOf(String tableName) {
		String[] colType = null;
		if(tables.containsKey(tableName)) {
			CreateTable tmp = tables.get(tableName);
			List<ColumnDefinition> columnNames = tmp.getColumnDefinitions();
			colType = new String[columnNames.size()];
			int i = 0;
			for(ColumnDefinition x : columnNames) {
				ColDataType colDataType =  x.getColDataType();
				colType[i] = colDataType.getDataType();
				i++;
			}
		}	
		return colType;
	}
	private static boolean readBatch(BufferedReader reader, String[] colType) throws IOException{
		ArrayList<ArrayList<PrimitiveValue>> unfilteredRows = new ArrayList<ArrayList<PrimitiveValue>>();
		for(int i = 0; i < BATCH_SIZE; i++) {
			String line = reader.readLine();
			if(line != null){
				String[] row = line.split("\\|");
				int j = 0;
				ArrayList<PrimitiveValue> tmp = new ArrayList<PrimitiveValue>();
				for(String x : row) {
					if(colType[j].equals("string") || colType[j].equals("varchar") || colType[j].equals("char")) {
						StringValue val = new StringValue(x);
						tmp.add(val);
						j++;
					}
					else if(colType[j].equals("int")){
						LongValue val = new LongValue(x);
						tmp.add(val);
						j++;
					}
					else if(colType[j].equals("decimal")) {
						DoubleValue val = new DoubleValue(x);
						tmp.add(val);
						j++;
					}
					else if(colType[j].equals("date")){
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
		for(ArrayList<PrimitiveValue> row : unfilteredRows) {
			if(FilterRows.filterRow(row,exp)) {
				filteredRows.add(row);
			}
		}
		return true;
	}
	
	
	private static void readCSV(String tableName) {
		String[] colType = typeOf(tableName);
		String path = "/Users/Kamran/Desktop/data/"+tableName+".csv";
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(path));
			boolean flag = true;
			while(flag) {
				flag = readBatch(br, colType);	
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
	}
		
	public static void createTable(CreateTable table) {
		if (!tables.containsKey(table.getTable().getName())) {
			tables.put(table.getTable().getName(), table);
		}
	}
	
	private static void createSchema(CreateTable queryStatement) {
		Integer j = 0;
		List<ColumnDefinition> q = queryStatement.getColumnDefinitions();
		for(ColumnDefinition c : q) {
			schema.put(c.getColumnName(), j);
			j++;
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
