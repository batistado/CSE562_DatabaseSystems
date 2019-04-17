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
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

public class Indexer {
	public static Map<String, BPlusTree> indexMapping = new HashMap<>();

	public static void addIndexes(CreateTable createTable) {
		if (createTable.getIndexes() != null) {
			for (Index tableIndex : createTable.getIndexes()) {
				List<Column> indexOnColumns = new ArrayList<Column>();
				for (String colName : tableIndex.getColumnsNames()) {
					indexOnColumns.add(new Column(createTable.getTable(), colName));
				}

				boolean isSorted = tableIndex.getType().equals("PRIMARY KEY");
				BPlusTree index = new BPlusTree(createTable.getTable(), indexOnColumns, isSorted);
				fillIndex(index, createTable.getTable());

				indexMapping.put(tableIndex.getColumnsNames().get(0), index);

			}
		}
	}

	public static void fillIndex(BPlusTree index, Table table) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(RAIterator.DIR + utils.getTableName(table) + ".csv"));
			String line = null;
			while ((line = br.readLine()) != null) {
				//buffer.add(utils.splitLine(line, table));
				index.insert(utils.splitLine(line, table));

			}
			index.closeIndex();
			
			System.gc();
			
			//RandomAccessFile raf = new RandomAccessFile(new File(RAIterator.DIR + utils.getTableName(table) + ".csv"), "r");
			
			br.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//				index.insert(utils.splitLine(line, table));
	}
}
//			index.closeIndex();
