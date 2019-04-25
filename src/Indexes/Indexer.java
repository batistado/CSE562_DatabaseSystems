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
import java.util.Set;

import Iterators.FromIterator;
import Iterators.RAIterator;
import Utils.utils;
import dubstep.Main;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

public class Indexer {
	public static Map<String, LinearPrimaryIndex> indexMapping = new HashMap<>();
	public static Map<String, LinearSecondaryIndex> secondaryIndexMapping = new HashMap<>();
	public static Map<String, Integer> tableSizeMapping = new HashMap<>();

	public static int getBranchingFactor(String tableName) {
		switch (tableName) {
		case "LINEITEM":
			return 25000;
		case "ORDERS":
			return 5000;
		case "CUSTOMER":
			return 500;
		case "PART":
			return 800;
		case "SUPPLIER":
			return 100;
		case "PARTSUPP":
			return 5000;
		default:
			return 1000;
		}
	}

	public static void addIndexes(CreateTable createTable) {
		if (createTable.getIndexes() != null) {
			LinearPrimaryIndex primaryIndex = null;
			List<LinearSecondaryIndex> secondaryIndexes = new ArrayList<LinearSecondaryIndex>();

			for (Index tableIndex : createTable.getIndexes()) {
				List<Column> indexOnColumns = new ArrayList<Column>();
				for (String colName : tableIndex.getColumnsNames()) {
					indexOnColumns.add(new Column(createTable.getTable(), colName));
				}

				if (tableIndex.getType().equals("PRIMARY KEY")) {
					primaryIndex = new LinearPrimaryIndex(createTable.getTable(), indexOnColumns.get(0));

					indexMapping.put(
							utils.getTableName(createTable.getTable()) + "." + tableIndex.getColumnsNames().get(0),
							primaryIndex);
				} else {
					if (!createTable.getTable().getName().equals("LINEITEM")) {
						LinearSecondaryIndex secondaryIndex = new LinearSecondaryIndex(createTable.getTable(),
								indexOnColumns.get(0));

						secondaryIndexes.add(secondaryIndex);

						secondaryIndexMapping.put(
								utils.getTableName(createTable.getTable()) + "." + tableIndex.getColumnsNames().get(0),
								secondaryIndex);
					}
				}
			}

			fillIndexes(primaryIndex, secondaryIndexes);
			String tableName = createTable.getTable().getName();
			writeIndexesToDisk(tableName);
			writeSecondaryIndexesToDisk(tableName);
			System.gc();
		}

	}

	public static void writeIndexesToDisk(String tableName) {
		try {
			String line = "";
			for (String key : indexMapping.keySet()) {
				String tableNameKey = key.split("\\.")[0];
				if (tableNameKey.equals(tableName)) {
					BufferedWriter bw = new BufferedWriter(
							new FileWriter(RAIterator.TEMP_DIR + "Indexes/" + key + ".csv"));
					LinearPrimaryIndex index = indexMapping.get(key);
					bw.write("TABLE:" + tableName);
					bw.write("\n");
					bw.write("TYPE:" + Main.tableSchemas.get(tableName).getSchemaByName(key).getDataType());
					bw.write("\n");

					for (int i = 0; i < index.keys.size(); i++) {
						line = "";
						line += index.keys.get(i).toString() + ",";
						Position pos = index.positions.get(i);
						line += pos.startPosition + ",";
						line += pos.endPosition;
						bw.write(line);
						bw.write("\n");
					}
					bw.close();
					break;
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void writeSecondaryIndexesToDisk(String tableName) {
		try {
			String line = "";
			for (String key : secondaryIndexMapping.keySet()) {
				String tableNameKey = key.split("\\.")[0];

				if (tableNameKey.equals(tableName)) {
					BufferedWriter bw = new BufferedWriter(
							new FileWriter(RAIterator.TEMP_DIR + "SecondaryIndexes/" + key + ".csv"));
					LinearSecondaryIndex index = secondaryIndexMapping.get(key);
					bw.write("TABLE:" + tableName);
					bw.write("\n");
					bw.write("TYPE:" + Main.tableSchemas.get(tableName).getSchemaByName(key).getDataType());
					bw.write("\n");

					for (int i = 0; i < index.keys.size(); i++) {
						line = "";
						line += index.keys.get(i).toString() + ":";
						Set<Position> positions = index.positions.get(i);

						for (Position position : positions) {
							line += position.startPosition + ",";
						}

						line = line.substring(0, line.length() - 1);
						line += ";";

						for (Position position : positions) {
							line += position.endPosition + ",";
						}

						line = line.substring(0, line.length() - 1);
						bw.write(line);
						bw.write("\n");
					}
					bw.close();
					break;
				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void loadIndex() {
		try {
			File dir = new File(RAIterator.TEMP_DIR + "/Indexes/");

			for (File file : dir.listFiles()) {
				String fileName = file.getName();
				if (fileName.endsWith("csv")) {
					BufferedReader br = new BufferedReader(new FileReader(file));

					String line = "";

					LinearPrimaryIndex index = null;
					String columnName = "";
					String colDatatype = "";
					while ((line = br.readLine()) != null) {
						if (line.startsWith("TABLE")) {
							if (index != null) {
								indexMapping.put(columnName, index);
							}
							index = new LinearPrimaryIndex();
							columnName = fileName.substring(0, fileName.lastIndexOf("."));
						} else if (line.startsWith("TYPE")) {
							colDatatype = line.substring(5);
						} else {
							String[] splitLine = line.split(",");
							PrimitiveValue key = null;

							if (colDatatype.equals("string") || colDatatype.equals("varchar")
									|| colDatatype.equals("char")) {
								key = new StringValue(splitLine[0]);
							} else if (colDatatype.equals("int")) {
								key = new LongValue(splitLine[0]);
							} else if (colDatatype.equals("decimal")) {
								key = new DoubleValue(splitLine[0]);
							} else if (colDatatype.equals("date")) {
								key = new DateValue(splitLine[0]);
							}

							index.addRow(key, Long.parseLong(splitLine[1]), Long.parseLong(splitLine[2]));
						}
					}

					if (index != null) {
						indexMapping.put(columnName, index);
					}

					br.close();
				}
			}

			System.gc();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void loadSecondaryIndex() {
		try {

			File dir = new File(RAIterator.TEMP_DIR + "/SecondaryIndexes/");

			for (File file : dir.listFiles()) {
				String fileName = file.getName();
				if (fileName.endsWith("csv")) {

					BufferedReader br = new BufferedReader(new FileReader(file));

					String line = "";

					LinearSecondaryIndex index = null;
					String columnName = "";
					String colDatatype = "";
					while ((line = br.readLine()) != null) {
						if (line.startsWith("TABLE")) {
							if (index != null) {
								secondaryIndexMapping.put(columnName, index);
							}
							index = new LinearSecondaryIndex();
							columnName = fileName.substring(0, fileName.lastIndexOf("."));
						} else if (line.startsWith("TYPE")) {
							colDatatype = line.substring(5);
						} else {
							String[] splitLine = line.split(":");
							PrimitiveValue key = null;

							if (colDatatype.equals("string") || colDatatype.equals("varchar")
									|| colDatatype.equals("char")) {
								key = new StringValue(splitLine[0]);
							} else if (colDatatype.equals("int")) {
								key = new LongValue(splitLine[0]);
							} else if (colDatatype.equals("decimal")) {
								key = new DoubleValue(splitLine[0]);
							} else if (colDatatype.equals("date")) {
								key = new DateValue(splitLine[0]);
							}

							String[] splitKVs = splitLine[1].split(";");

							String[] starts = splitKVs[0].split(",");
							String[] ends = splitKVs[1].split(",");

							index.addRow(key, starts, ends);
						}
					}

					if (index != null) {
						secondaryIndexMapping.put(columnName, index);
					}

					br.close();
				}
			}

			System.gc();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void fillIndexes(LinearPrimaryIndex index, List<LinearSecondaryIndex> secondaryIndexes) {
		try {
			Table table = index.table;
			FileInputStream fis = new FileInputStream(RAIterator.DIR + utils.getTableName(index.table) + ".csv");
			InputStreamReader isr = new InputStreamReader(fis);
			BufferedReader br = new BufferedReader(isr);
			String line = null;

			long pos = 0;
			while ((line = br.readLine()) != null) {
				ArrayList<PrimitiveValue> row = utils.splitLine(line, table);
				PrimitiveValue pKey = utils.projectColumnValue(row, index.column,
						Main.tableSchemas.get(utils.getTableName(table)));
				index.insert(pKey, pos);

				Position position = index.search(pKey);

				for (LinearSecondaryIndex si : secondaryIndexes) {
					PrimitiveValue sKey = utils.projectColumnValue(row, si.column,
							Main.tableSchemas.get(utils.getTableName(table)));
					si.insert(sKey, position);
				}

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
