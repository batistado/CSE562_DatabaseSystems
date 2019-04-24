package dubstep;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import Indexes.PrimaryIndex;
import Indexes.Indexer;
import Indexes.LinearPrimaryIndex;
import Iterators.RAIterator;
import Utils.*;
import Models.TupleSchema;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.select.Select;

public class Main {
	public static Map<String, TupleSchema> tableSchemas = new HashMap<>();
	public static boolean isInMemory;
	public static int sortedRunSize = 2;
	public static int sortBufferSize = 100000;
	public static int offset = 1;
	
	
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		isInMemory = false;
		
		for (String arg : args) {
			if (arg.equals("--in-mem")){
	            isInMemory = true;
	        }
		}
		
		Indexer.tableSizeMapping.put("LINEITEM", 6001215);
		Indexer.tableSizeMapping.put("ORDERS", 1500000);
		Indexer.tableSizeMapping.put("CUSTOMER", 150000);
		Indexer.tableSizeMapping.put("REGION", 5);
		Indexer.tableSizeMapping.put("NATION", 25);
		Indexer.tableSizeMapping.put("SUPPLIER", 10000);
		Indexer.tableSizeMapping.put("PART", 200000);
		Indexer.tableSizeMapping.put("PARTSUPP", 800000);
		//10000
		
		
		
		// Check and make a temp dir
		File directory = new File(RAIterator.TEMP_DIR);
	    if (!directory.exists()){
	        directory.mkdir();
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
		Indexer.addIndexes(table);
		writeSchemaToDisk();
	}
	
	public static void writeSchemaToDisk() {
		try {
			ObjectOutputStream indexWriter = new ObjectOutputStream(new FileOutputStream(RAIterator.TEMP_DIR + "Schema.csv"));
			indexWriter.writeObject(tableSchemas);
			indexWriter.reset();
			indexWriter.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static RAIterator evaluateQuery(Select selectQuery) {
		if (Main.tableSchemas.isEmpty()) {
			try {
//				ObjectInputStream ois = new ObjectInputStream(new FileInputStream(RAIterator.TEMP_DIR + "Index.csv"));
//				Indexer.indexMapping = (Map<String, LinearPrimaryIndex>) ois.readObject();
//				ois.close();
				
				ObjectInputStream ois = new ObjectInputStream(new FileInputStream(RAIterator.TEMP_DIR + "Schema.csv"));
				tableSchemas = (Map<String, TupleSchema>) ois.readObject();
				ois.close();
				
				Indexer.loadIndex();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException ex) {
				ex.printStackTrace();
			}
		}
		
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
