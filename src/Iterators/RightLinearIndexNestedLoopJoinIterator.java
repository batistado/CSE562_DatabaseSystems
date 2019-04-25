package Iterators;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import Indexes.LinearPrimaryIndex;
import Indexes.Position;
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

public class RightLinearIndexNestedLoopJoinIterator implements RAIterator {
	private FromIterator rightIterator = null;
	private RAIterator leftIterator = null;
	private ArrayList<PrimitiveValue> rightRow;
	private ArrayList<PrimitiveValue> leftRow;
	private TupleSchema fromSchema;
	private TupleSchema selectSchema;
	private LinearPrimaryIndex index;
	private Expression joinCondition;
	private BufferedReader reader;
	private Expression rightExpression;
	private BinaryExpression joinOn;
	private Position position;
	private FileInputStream fis;
	private long currPosition;

	public RightLinearIndexNestedLoopJoinIterator(FromIterator rightIterator, RAIterator leftIterator, LinearPrimaryIndex index,
			Expression joinCondition) {
		this.rightIterator = rightIterator;
		this.leftIterator = leftIterator;
		this.joinCondition = joinCondition;
		this.index = index;
		this.rightExpression = utils.getSingleColumnExpression(joinCondition, leftIterator.getIteratorSchema());
		setIteratorSchema();
		initializeReader();
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
		this.position = null;
		rightRow = null;
		leftRow = null;
		
		try {
			if (reader != null) {
				reader.close();
				reader = null;
			}
		initializeReader();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void initializeReader() {
		try {
			fis = new FileInputStream(DIR + rightIterator.getTable().getName() + ".csv");
			fis.getChannel().position(0);
			InputStreamReader isr = new InputStreamReader(fis);
			reader = new BufferedReader(isr);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void seekToPosition(long newPosition) {
		try {
			fis.getChannel().position(newPosition);
			InputStreamReader isr = new InputStreamReader(fis);
			reader = new BufferedReader(isr);
		} catch (IOException e) {
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

		if (position == null) {
			getPosition();
			if (this.position == null) {
				return false;
			}
			
			currPosition = position.startPosition;
			seekToPosition(currPosition);
		}

		

		try {
			while (currPosition <= position.endPosition && (rightRow = getLeftRow(reader.readLine())) != null) {
				if (utils.filterRow(rightRow, joinOn, rightIterator.getIteratorSchema()))
					return true;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			while (leftIterator.hasNext()) {
				getPosition();
				
				if (this.position == null) {
					return false;
				}

				currPosition = position.startPosition;
				seekToPosition(currPosition);
				while (currPosition <= position.endPosition && (rightRow = getLeftRow(reader.readLine())) != null) {
					if (utils.filterRow(rightRow, joinOn, rightIterator.getIteratorSchema()))
						return true;
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		leftRow = null;
		try {
			reader.close();
			reader = null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	private void getPosition() {
		leftRow = leftIterator.next();

		PrimitiveValue searchKey = utils.projectColumnValue(leftRow, rightExpression,
				leftIterator.getIteratorSchema());

		joinOn = (BinaryExpression) joinCondition;
		joinOn.setRightExpression(searchKey);

		List<TreeSearch> treeSearchObjects = utils.getSearchObject(joinOn);

		for (TreeSearch treeSearchObject : treeSearchObjects) {
			if (treeSearchObject.operation.equals("EQUALS")) {
				this.position = index.search(treeSearchObject.leftValue);
			} else {
				this.position = index.searchRange(treeSearchObject.leftValue, treeSearchObject.leftPolicy,
						treeSearchObject.rightValue, treeSearchObject.rightPolicy);
			}
		}
	}

	public ArrayList<PrimitiveValue> getLeftRow(String line) {
		if (line == null) {
			return null;
		}

		currPosition += line.length() + Main.offset;
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
		TupleSchema rightIteratorSchema, leftIteratorSchema;
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


