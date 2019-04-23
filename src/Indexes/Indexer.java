package Indexes;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import Iterators.FromIterator;
import Iterators.RAIterator;
import Utils.utils;
import dubstep.Main;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

public class Indexer {
	public static Map<String, LinearPrimaryIndex> indexMapping = new HashMap<>();
	
	public static int getBranchingFactor(String tableName) {
		switch (tableName) {
		case "LINEITEM": return 25000;
		case "ORDERS": return 5000;
		case "CUSTOMER": return 500;
		case "PART": return 800;
		case "SUPPLIER": return 100;
		case "PARTSUPP": return 5000;
		default: return 1000;
		}
	}

	public static void addIndexes(CreateTable createTable) {
		if (createTable.getIndexes() != null) {
			for (Index tableIndex : createTable.getIndexes()) {
				List<Column> indexOnColumns = new ArrayList<Column>();
				for (String colName : tableIndex.getColumnsNames()) {
					indexOnColumns.add(new Column(createTable.getTable(), colName));
				}

				if (tableIndex.getType().equals("PRIMARY KEY")) {
					Table table = createTable.getTable();
					
					//PrimaryIndex index = new PrimaryIndex(table, indexOnColumns, getBranchingFactor(table.getName()));
					LinearPrimaryIndex index = new LinearPrimaryIndex();
					fillIndex(index, createTable.getTable(), indexOnColumns.get(0));
	
					indexMapping.put(utils.getTableName(createTable.getTable()) + "." + tableIndex.getColumnsNames().get(0), index);
					indexMapping.put(tableIndex.getColumnsNames().get(0), index);
				}
			}
		}
		
		try {
			ObjectOutputStream indexWriter = new ObjectOutputStream(new FileOutputStream(RAIterator.TEMP_DIR + "Index.csv"));
			indexWriter.writeObject(indexMapping);
			indexWriter.reset();
			indexWriter.close();
			System.gc();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void fillIndex(LinearPrimaryIndex index, Table table, Column col) {
		try {
			FileInputStream fis = new FileInputStream(RAIterator.DIR + utils.getTableName(table) + ".csv");
			InputStreamReader isr = new InputStreamReader(fis);
			BufferedReader br = new BufferedReader(isr);
			String line = null;
			
			long pos = 0;
			while ((line = br.readLine()) != null) {
				index.insert(utils.splitLine(line, table), col, table, pos);
				pos += line.length() + Main.offset;
			}
			br.close();
			
			System.gc();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
	}
}		
