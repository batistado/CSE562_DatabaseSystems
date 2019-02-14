package dubstep;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class Main {
	private static Map<String, CreateTable> tables = new HashMap<String, CreateTable>();
	private static Map<String, TupleSchema> tupleSchema = new HashMap<String, TupleSchema>();
	private static final String DATA_DIR = "data";
	private static BufferedReader br = null;
	private static final String FS = "\\|";

	public static void main(String[] args) {
		CCJSqlParser parser;
		System.out.println("$> ");
		parser = new CCJSqlParser(System.in);
		Statement queryStatement;
		try {
			while ((queryStatement = parser.Statement()) != null) {
				if (queryStatement instanceof Select) {
					evaluateQuery((Select) queryStatement);
				} else if (queryStatement instanceof CreateTable) {
					createTable((CreateTable) queryStatement);
				}
				System.out.println("$> ");
				parser = new CCJSqlParser(System.in);
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			System.out.println("Can't read query" + args[0]);
			e.printStackTrace();
		}
	}
	
	public void loadCsv(String tableName) {
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream("/" + DATA_DIR + "/" + tableName)));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("Table "+ tableName + " not found");
			e.printStackTrace();
		}
	}
	
	public void readLine(String tableName) {
		if (br == null) {
			loadCsv(tableName);
		}
		
		String strLine;
		try {
			if ((strLine = br.readLine()) != null)   {
			  String[] rowValues = strLine.split("|");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void createTable(CreateTable table) {
		if (!tables.containsKey(table.getTable().getName())) {
			tables.put(table.getTable().getName(), table);
			TupleSchema ts = new TupleSchema();
			Integer index = 0;
			for (ColumnDefinition columnDef : table.getColumnDefinitions()) {
				ts.addTuple(columnDef.getColumnName(), index);
				index++;
			}
			tupleSchema.put(table.getTable().getName(), ts);
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
			evaluateSinglePlainSelect(plainSelectQuery);
		}
	}
	
	public static void filter(Expression expression, Table table) {
		Eval eval = new Eval() {
			public PrimitiveValue eval(Column col){
				return new LongValue(6);
			}
		};
		
		try {
			eval.eval(expression);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void evaluateSinglePlainSelect(PlainSelect plainSelect) {
		Expression where = plainSelect.getWhere();
		
		if (where != null) {
			//filter(where, (Table)plainSelect.getFromItem());
		}
	}
	
	public static void evaluateQuery(Select selectQuery) {
		if (selectQuery.getSelectBody() instanceof PlainSelect) {
			evaluatePlainSelect((PlainSelect)selectQuery.getSelectBody());
		} else {
			// Write Union logic
		}
	}

	public static void printer(String tableName) {
		String path = "data/" + tableName + ".csv";
		String line;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(path));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			while ((line = br.readLine()) != null) {
				System.out.println(line);
			}
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
