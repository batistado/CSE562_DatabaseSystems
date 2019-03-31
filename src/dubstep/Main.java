package dubstep;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import Iterators.RAIterator;
import Utils.*;
import Models.TupleSchema;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

public class Main {
	public static Map<String, TupleSchema> tableSchemas = new HashMap<>();
	public static boolean isInMemory;
	public static int sortedRunSize = 10;
	public static int sortBufferSize = 5000;
	
	
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		isInMemory = false;
		
		for (String arg : args) {
			if (arg.equals("--in-mem")){
	            isInMemory = true;
	        }
		}
		
		
		CCJSqlParser parser;
		
		System.out.println("$> ");
		parser = new CCJSqlParser(System.in);
		Statement queryStatement;
		try {
			while ((queryStatement = parser.Statement()) != null) {
				if (queryStatement instanceof Select) {
					Select selectQuery = (Select) queryStatement;
					RAIterator queryIterator = evaluateQuery(selectQuery);
					printer(queryIterator);
					queryIterator = null;
				}
				else if (queryStatement instanceof CreateTable) {
					createTable((CreateTable) queryStatement);
				}
				//System.gc();
				System.out.println("$> ");
				parser = new CCJSqlParser(System.in);
			}
		} catch (ParseException e) {
			System.out.println("Can't read query" + args[0]);
			e.printStackTrace();
		}
	}
		
	public static void createTable(CreateTable table) {
		TupleSchema ts = new TupleSchema();
		Integer i = 0;
		for (ColumnDefinition columnDefinition : table.getColumnDefinitions()) {
			ts.addTuple(table.getTable().getName() + "." + columnDefinition.getColumnName(), i, columnDefinition.getColDataType().getDataType().toLowerCase());
			i++;
		}
		tableSchemas.put(table.getTable().getName(), ts);
	}
	
	public static RAIterator evaluateQuery(Select selectQuery) {
		return new Optimizer().optimizeRA(new QueryEvaluator().evaluateQuery(selectQuery));
		//return new QueryEvaluator().evaluateQuery(selectQuery);
	}
	
	public static void printer(RAIterator iterator) throws FileNotFoundException, UnsupportedEncodingException {
		while (iterator.hasNext()) {
			System.out.println(utils.getOutputString(iterator.next()));
		}
		System.out.println();
	}
}
