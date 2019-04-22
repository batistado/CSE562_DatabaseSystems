package Iterators;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import Indexes.PrimaryIndex;
import Indexes.TreeSearch;
import Models.Schema;
import Models.TupleSchema;
import Utils.utils;
import dubstep.Main;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.select.SelectItem;

public class LeftIndexNestedLoopJoinIterator implements RAIterator {
	private FromIterator leftIterator = null;
	private RAIterator rightIterator = null;
	private ArrayList<PrimitiveValue> leftRow;
	private ArrayList<PrimitiveValue> rightRow;
	private TupleSchema fromSchema;
	private TupleSchema selectSchema;
	private PrimaryIndex tree;
	private LinkedList<String> leftFileNames;
	private Expression joinCondition;
	private BufferedReader leftFileReader;
	private Expression rightExpression;
	private BinaryExpression joinOn;

	public LeftIndexNestedLoopJoinIterator(FromIterator leftIterator, RAIterator rightIterator, PrimaryIndex tree,
			Expression joinCondition) {
		this.leftIterator = leftIterator;
		this.rightIterator = rightIterator;
		this.joinCondition = joinCondition;
		this.tree = tree;
		this.rightExpression = utils.getSingleColumnExpression(joinCondition, rightIterator.getIteratorSchema());
		setIteratorSchema();
		// System.gc();
	}

	@Override
	public void resetWhere() {
	}

	@Override
	public void resetProjection() {
	}

	public void resetIterator() {
		this.rightIterator.resetIterator();
		this.leftFileNames.clear();
		leftRow = null;
		rightRow = null;
	}

	private void readNextFile() {
		try {
			leftFileReader = new BufferedReader(new FileReader(TEMP_DIR + leftFileNames.poll()));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		if (rightRow == null && !rightIterator.hasNext()) {
			return false;
		}

		if (leftFileReader == null) {
			refillLeftBuffer();

			if (leftFileNames.size() == 0) {
				return false;
			}

			readNextFile();
		}

		try {
			while (true) {
				while ((leftRow = getLeftRow(leftFileReader.readLine())) != null) {
					if (utils.filterRow(leftRow, joinOn, leftIterator.getIteratorSchema()))
						return true;
				}

				if (leftFileNames.size() == 0) {
					break;
				}

				readNextFile();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			while (rightIterator.hasNext()) {
				refillLeftBuffer();

				while (true) {
					while ((leftRow = getLeftRow(leftFileReader.readLine())) != null) {
						if (utils.filterRow(leftRow, joinOn, leftIterator.getIteratorSchema()))
							return true;
					}

					if (leftFileNames.size() == 0) {
						break;
					}
					
					readNextFile();
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		rightRow = null;
		try {
			leftFileReader.close();
			leftFileReader = null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	private void refillLeftBuffer() {
		rightRow = rightIterator.next();

		HashSet<String> fileNames = new HashSet<String>();

		PrimitiveValue searchKey = utils.projectColumnValue(rightRow, rightExpression,
				rightIterator.getIteratorSchema());

		joinOn = (BinaryExpression) joinCondition;
		joinOn.setRightExpression(searchKey);

		List<TreeSearch> treeSearchObjects = utils.getSearchObject(joinOn);

		for (TreeSearch treeSearchObject : treeSearchObjects) {
			if (treeSearchObject.operation.equals("EQUALS")) {
				fileNames.add(tree.search(treeSearchObject.leftValue));
			} else {
				fileNames.addAll(tree.searchRange(treeSearchObject.leftValue, treeSearchObject.leftPolicy,
						treeSearchObject.rightValue, treeSearchObject.rightPolicy));
			}
		}

		this.leftFileNames = new LinkedList<String>(fileNames);
	}

	public ArrayList<PrimitiveValue> getLeftRow(String line) {
		if (line == null) {
			return null;
		}

		String[] row = line.split("\\|");
		int j = 0;
		ArrayList<PrimitiveValue> tmp = new ArrayList<PrimitiveValue>();
		for (String x : row) {
			String colDatatype = Main.tableSchemas.get(leftIterator.getTable().getName()).getSchemaByIndex(j)
					.getDataType();
			if (colDatatype.equals("string") || colDatatype.equals("varchar") || colDatatype.equals("char")) {
				StringValue val = new StringValue(x);
				tmp.add(val);
			} else if (colDatatype.equals("int")) {
				LongValue val = new LongValue(x);
				tmp.add(val);
			} else if (colDatatype.equals("decimal")) {
				DoubleValue val = new DoubleValue(x);
				tmp.add(val);
			} else if (colDatatype.equals("date")) {
				DateValue val = new DateValue(x);
				tmp.add(val);
			}

			j++;

		}

		return tmp;
	}

	@Override
	public ArrayList<PrimitiveValue> next() {
		// TODO Auto-generated method stub
		ArrayList<PrimitiveValue> mergedRow = new ArrayList<>();
		mergedRow.addAll(leftRow);
		mergedRow.addAll(rightRow);
		return mergedRow;
	}

	public TupleSchema getIteratorSchema() {
		return fromSchema;
	}

	public TupleSchema getSelectSchema() {
		return selectSchema;
	}

	public void setIteratorSchema() {
		TupleSchema leftIteratorSchema, rightIteratorSchema;
		leftIteratorSchema = leftIterator.getIteratorSchema();
		rightIteratorSchema = rightIterator.getIteratorSchema();
		
		fromSchema = new TupleSchema();

		Map<String, Schema> schemaByName = leftIteratorSchema.schemaByName();
		Integer maxIndex = -1;

		for (String name : schemaByName.keySet()) {
			String colName = name;
			Schema s = leftIteratorSchema.getSchemaByName(name);
			fromSchema.addTuple(colName, s.getColumnIndex(), s.getDataType());

			if (s.getColumnIndex() > maxIndex) {
				maxIndex = s.getColumnIndex();
			}
		}

		schemaByName = rightIteratorSchema.schemaByName();
		for (String name : schemaByName.keySet()) {
			String colName = name;
			Schema s = rightIteratorSchema.getSchemaByName(name);
			fromSchema.addTuple(colName, s.getColumnIndex() + maxIndex + 1, s.getDataType());
		}
	}

	@Override
	public void addSelectItems(List<SelectItem> selectItems) {
		// TODO Auto-generated method stub

	}

	@Override
	public RAIterator getLeftIterator() {
		// TODO Auto-generated method stub
		return leftIterator;
	}

	@Override
	public RAIterator getRightIterator() {
		// TODO Auto-generated method stub
		return rightIterator;
	}
}
