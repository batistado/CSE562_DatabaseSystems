package Indexes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import Iterators.FromIterator;
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
				
				BPlusTree index = new BPlusTree(createTable.getTable(), indexOnColumns);
				fillIndex(index, createTable.getTable());
				
				
				indexMapping.put(tableIndex.getColumnsNames().get(0), index);
				System.gc();
				
			}
		}
	}
	
	public static void fillIndex(BPlusTree index, Table table) {
		FromIterator iterator = new FromIterator(table);
		
		while (iterator.hasNext()) {
			index.insert(iterator.next());
		}
		index.closeIndex();
	}
}
