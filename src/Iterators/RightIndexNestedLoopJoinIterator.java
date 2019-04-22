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

public class RightIndexNestedLoopJoinIterator implements RAIterator {
	private FromIterator rightIterator = null;
	private RAIterator leftIterator = null;
	private ArrayList<PrimitiveValue> rightRow;
	private ArrayList<PrimitiveValue> leftRow;
	private TupleSchema fromSchema;
	private TupleSchema selectSchema;
	private PrimaryIndex tree;
	private LinkedList<String> rightFileNames;
	private Expression joinCondition;
	private BufferedReader rightFileReader;
	private Expression rightExpression;
	private BinaryExpression joinOn;

	public RightIndexNestedLoopJoinIterator(FromIterator rightIterator, RAIterator leftIterator, PrimaryIndex tree,
			Expression joinCondition) {
		this.rightIterator = rightIterator;
		this.leftIterator = leftIterator;
		this.joinCondition = joinCondition;
		this.tree = tree;
		this.rightExpression = utils.getSingleColumnExpression(joinCondition, leftIterator.getIteratorSchema());
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
		this.leftIterator.resetIterator();
		this.rightFileNames.clear();
		rightRow = null;
		leftRow = null;
	}

	private void readNextFile() {
		try {
			rightFileReader = new BufferedReader(new FileReader(TEMP_DIR + rightFileNames.poll()));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		if (leftRow == null && !leftIterator.hasNext()) {
			return false;
		}

		if (rightFileReader == null) {
			refillRightBuffer();

			if (rightFileNames.size() == 0) {
				return false;
			}

			readNextFile();
		}

		try {
			while (true) {
				while ((rightRow = getRightRow(rightFileReader.readLine())) != null) {
					if (utils.filterRow(rightRow, joinOn, rightIterator.getIteratorSchema()))
						return true;
				}

				if (rightFileNames.size() == 0) {
					break;
				}

				readNextFile();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			while (leftIterator.hasNext()) {
				refillRightBuffer();

				while (true) {
					while ((rightRow = getRightRow(rightFileReader.readLine())) != null) {
						if (utils.filterRow(rightRow, joinOn, rightIterator.getIteratorSchema()))
							return true;
					}

					if (rightFileNames.size() == 0) {
						break;
					}
					
					readNextFile();
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		leftRow = null;
		try {
			rightFileReader.close();
			rightFileReader = null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	private void refillRightBuffer() {
		leftRow = leftIterator.next();

		HashSet<String> fileNames = new HashSet<String>();

		PrimitiveValue searchKey = utils.projectColumnValue(leftRow, rightExpression,
				leftIterator.getIteratorSchema());

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

		this.rightFileNames = new LinkedList<String>(fileNames);
	}

	public ArrayList<PrimitiveValue> getRightRow(String line) {
		if (line == null) {
			return null;
		}

		String[] row = line.split("\\|");
		int j = 0;
		ArrayList<PrimitiveValue> tmp = new ArrayList<PrimitiveValue>();
		for (String x : row) {
			String colDatatype = Main.tableSchemas.get(rightIterator.getTable().getName()).getSchemaByIndex(j)
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
		rightIteratorSchema = rightIterator.getIteratorSchema();
		leftIteratorSchema = leftIterator.getIteratorSchema();
		
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
		return rightIterator;
	}

	@Override
	public RAIterator getRightIterator() {
		// TODO Auto-generated method stub
		return leftIterator;
	}
}

