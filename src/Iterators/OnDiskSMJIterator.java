package Iterators;

import java.io.*;
import java.util.*;
import java.util.List;
import java.util.Map;

import Models.Schema;
import Models.TupleSchema;
import Utils.Sort;
import Utils.utils;
import dubstep.Main;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectItem;

public class OnDiskSMJIterator implements RAIterator {
	private RAIterator leftIterator = null;
	private RAIterator rightIterator = null;
	private ArrayList<PrimitiveValue> row;
	private TupleSchema fromSchema;
	private TupleSchema selectSchema;
	private Expression joinCondition;
	private String leftFileName;
	private String rightFileName;
	private ArrayList<ArrayList<PrimitiveValue>> leftBuffer;
	private ArrayList<ArrayList<PrimitiveValue>> rightBuffer;
	private LinkedList<ArrayList<PrimitiveValue>> buffer;
	private BufferedReader leftReader;
	private BufferedReader rightReader;
	private ArrayList<PrimitiveValue> leftRow;
	private ArrayList<PrimitiveValue> rightRow;
	private boolean hasMatch;

	public OnDiskSMJIterator(RAIterator leftIterator, RAIterator rightIterator, Expression joinCondition) {
		this.leftBuffer = new ArrayList<ArrayList<PrimitiveValue>>();
		this.rightBuffer = new ArrayList<ArrayList<PrimitiveValue>>();
		this.buffer = new LinkedList<ArrayList<PrimitiveValue>>();
		this.hasMatch = false;
		this.joinCondition = joinCondition;
		this.leftIterator = leftIterator;
		this.rightIterator = rightIterator;
		this.setIteratorSchema();
		this.initializeIterator();
		this.initializeReader();
		// System.gc();
	}

	private void initializeReader() {
		// TODO Auto-generated method stub
		try {
			row = new ArrayList<PrimitiveValue>();
			leftRow = new ArrayList<PrimitiveValue>();
			rightRow = new ArrayList<PrimitiveValue>();
			leftReader = new BufferedReader(new FileReader(leftFileName));
			rightReader = new BufferedReader(new FileReader(rightFileName));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("File names not found " + leftFileName + " " + rightFileName);
			e.printStackTrace();
		}
	}

	public Expression getExpression() {
		return this.joinCondition;
	}

	public void initializeIterator() {
		EqualsTo equalsToExpression = (EqualsTo) joinCondition;

		OrderByElement order = new OrderByElement();
		order.setExpression((Column) equalsToExpression.getLeftExpression());
		order.setAsc(true);
		List<OrderByElement> orderByElements = new ArrayList<OrderByElement>();
		orderByElements.add(order);
		leftFileName = sort(leftIterator, orderByElements, leftBuffer);
		leftBuffer.clear();

		order.setExpression((Column) equalsToExpression.getRightExpression());
		order.setAsc(true);
		orderByElements.clear();
		orderByElements.add(order);
		rightFileName = sort(rightIterator, orderByElements, rightBuffer);
		rightBuffer.clear();
	}

	public String sort(RAIterator iterator, List<OrderByElement> orderByElements,
			ArrayList<ArrayList<PrimitiveValue>> buffer) {
		return new Sort().sortData(iterator, orderByElements, iterator.getIteratorSchema(), DIR, buffer, false);
	}

	@Override
	public void resetWhere() {
	}

	@Override
	public void resetProjection() {
	}

	public void resetIterator() {
		initializeReader();
	}

	private ArrayList<PrimitiveValue> getRow(boolean isLeftLine, String line) {
		// TODO Auto-generated method stub
		if (line == null)
			return null;

		TupleSchema schema = null;

		if (isLeftLine) {
			schema = leftIterator.getIteratorSchema();
		} else {
			schema = rightIterator.getIteratorSchema();
		}

		String[] row = line.split("\\|");
		int j = 0;
		ArrayList<PrimitiveValue> tmp = new ArrayList<PrimitiveValue>();
		for (String x : row) {
			String colDatatype = schema.getSchemaByIndex(j).getDataType();
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

	public void fillBuffer() throws IOException {
		// on-disk

		leftBuffer = new ArrayList<ArrayList<PrimitiveValue>>();
		rightBuffer = new ArrayList<ArrayList<PrimitiveValue>>();
		leftBuffer.add(leftRow);
		rightBuffer.add(rightRow);

		PrimitiveValue joinValue = utils.projectColumnValue(row, ((EqualsTo) joinCondition).getLeftExpression(),
				fromSchema);
		// fill up left buffer:

		Column column = (Column) ((EqualsTo) joinCondition).getLeftExpression();
		while (leftReader != null && (leftRow = getRow(true, leftReader.readLine())) != null) {
			if (!utils.areEqual(joinValue, leftRow.get(
					leftIterator.getIteratorSchema().getSchemaByName(utils.getColumnName(column)).getColumnIndex()))) {
				break;
			}

			leftBuffer.add(leftRow);
		}

		column = (Column) ((EqualsTo) joinCondition).getRightExpression();
		while (rightReader != null && (rightRow = getRow(false, rightReader.readLine())) != null) {
			if (!utils.areEqual(joinValue, rightRow.get(
					rightIterator.getIteratorSchema().getSchemaByName(utils.getColumnName(column)).getColumnIndex()))) {
				break;
			}

			rightBuffer.add(rightRow);
		}

		// Cross product
		ArrayList<PrimitiveValue> tmp;
		int leftIndex = 0;
		while (leftIndex < leftBuffer.size()) {
			int rightIndex = 0;
			while (rightIndex < rightBuffer.size()) {
				tmp = new ArrayList<PrimitiveValue>();
				tmp.addAll(leftBuffer.get(leftIndex));
				tmp.addAll(rightBuffer.get(rightIndex));

				buffer.add(tmp);
				rightIndex++;
			}
			leftIndex++;
		}
	}

	@Override
	public boolean hasNext() {
		try {
			// On-disk
			if (!buffer.isEmpty()) {
				row = buffer.pollFirst();
				return true;
			}

			if (hasMatch) {
				if (leftRow == null || rightRow == null) {
					leftRow = null;
					rightRow = null;

					if (leftReader != null) {
						leftReader.close();
						leftReader = null;
					}

					if (rightReader != null) {
						rightReader.close();
						rightReader = null;
					}
					row = null;
					hasMatch = false;
					return false;
				}
				hasMatch = false;
			} else if (leftReader == null || rightReader == null
					|| (leftRow = getRow(true, leftReader.readLine())) == null
					|| (rightRow = getRow(false, rightReader.readLine())) == null) {
				if (leftReader != null) {
					leftReader.close();
					leftReader = null;
				}

				if (rightReader != null) {
					rightReader.close();
					rightReader = null;
				}

				row = null;
				return false;
			}

			while (leftRow != null && rightRow != null) {
				row = new ArrayList<PrimitiveValue>();

				row.addAll(leftRow);
				row.addAll(rightRow);

				if (utils.filterRow(row, joinCondition, fromSchema)) {
					fillBuffer();
					row = buffer.pollFirst();
					hasMatch = true;
					return true;
				}

				EqualsTo equalsToExpression = (EqualsTo) joinCondition;
				Expression e = new GreaterThan(equalsToExpression.getLeftExpression(),
						equalsToExpression.getRightExpression());

				if (utils.filterRow(row, e, fromSchema)) {
					if ((rightRow = getRow(false, rightReader.readLine())) == null) {
						rightReader.close();
						leftReader.close();
						leftReader = null;
						rightReader = null;
						rightRow = null;
						leftRow = null;
						return false;
					}
				} else {
					if ((leftRow = getRow(true, leftReader.readLine())) == null) {
						rightReader.close();
						leftReader.close();
						leftReader = null;
						rightReader = null;
						rightRow = null;
						leftRow = null;
						return false;
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return false;
	}

	@Override
	public ArrayList<PrimitiveValue> next() {
		// TODO Auto-generated method stub
		return row;
	}

	public TupleSchema getIteratorSchema() {
		return fromSchema;
	}

	public TupleSchema getSelectSchema() {
		return selectSchema;
	}

	public void setIteratorSchema() {
		TupleSchema leftIteratorSchema = leftIterator.getIteratorSchema();
		TupleSchema rightIteratorSchema = rightIterator.getIteratorSchema();
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