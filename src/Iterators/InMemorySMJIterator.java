package Iterators;

import java.util.*;
import java.util.List;
import java.util.Map;

import Models.Schema;
import Models.TupleSchema;
import Utils.Sort;
import Utils.utils;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectItem;

public class InMemorySMJIterator implements RAIterator {
	private RAIterator leftIterator = null;
	private RAIterator rightIterator = null;
	private ArrayList<PrimitiveValue> row;
	private TupleSchema fromSchema;
	private TupleSchema selectSchema;
	private Expression joinCondition;
	private ArrayList<ArrayList<PrimitiveValue>> leftBuffer;
	private ArrayList<ArrayList<PrimitiveValue>> rightBuffer;
	private LinkedList<ArrayList<PrimitiveValue>> buffer;
	private Integer leftBufferIndex;
	private Integer rightBufferIndex;

	public InMemorySMJIterator(RAIterator leftIterator, RAIterator rightIterator, Expression joinCondition) {
		this.leftBuffer = new ArrayList<ArrayList<PrimitiveValue>>();
		this.rightBuffer = new ArrayList<ArrayList<PrimitiveValue>>();
		this.buffer = new LinkedList<ArrayList<PrimitiveValue>>();
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
		row = new ArrayList<PrimitiveValue>();
		leftBufferIndex = -1;
		rightBufferIndex = -1;
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
		sort(leftIterator, orderByElements, leftBuffer);

		order.setExpression((Column) equalsToExpression.getRightExpression());
		order.setAsc(true);
		orderByElements.clear();
		orderByElements.add(order);
		sort(rightIterator, orderByElements, rightBuffer);
	}

	public String sort(RAIterator iterator, List<OrderByElement> orderByElements,
			ArrayList<ArrayList<PrimitiveValue>> buffer) {
		return new Sort().sortData(iterator, orderByElements, iterator.getIteratorSchema(), DIR, buffer, true);
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

	public void fillBuffer() {
		ArrayList<PrimitiveValue> tmp = new ArrayList<PrimitiveValue>();

		PrimitiveValue joinValue = utils.projectColumnValue(leftBuffer.get(leftBufferIndex),
				((EqualsTo) joinCondition).getLeftExpression(), fromSchema);

		int rightIndex = rightBufferIndex, maxRight = rightBufferIndex;
		while (leftBufferIndex < leftBuffer.size()
				&& utils.areEqual(joinValue, utils.projectColumnValue(leftBuffer.get(leftBufferIndex),
						((EqualsTo) joinCondition).getLeftExpression(), fromSchema))) {
			rightBufferIndex = rightIndex;
			while (rightBufferIndex < rightBuffer.size()) {

				tmp = new ArrayList<PrimitiveValue>();
				tmp.addAll(leftBuffer.get(leftBufferIndex));
				tmp.addAll(rightBuffer.get(rightBufferIndex));
				if (utils.filterRow(tmp, joinCondition, fromSchema)) {
					buffer.add(tmp);
				} else {
					break;
				}

				rightBufferIndex++;
				if (maxRight < rightBufferIndex) {
					maxRight = rightBufferIndex;
				}
			}
			leftBufferIndex++;
		}

		leftBufferIndex--;
		rightBufferIndex = maxRight - 1;
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		if (!buffer.isEmpty()) {
			row = buffer.pollFirst();
			return true;
		}

		if (++leftBufferIndex >= leftBuffer.size() || ++rightBufferIndex >= rightBuffer.size()) {
			row = null;
			return false;
		}

		while (leftBufferIndex < leftBuffer.size() && rightBufferIndex < rightBuffer.size()) {
			row = new ArrayList<PrimitiveValue>();

			row.addAll(leftBuffer.get(leftBufferIndex));
			row.addAll(rightBuffer.get(rightBufferIndex));

			if (utils.filterRow(row, joinCondition, fromSchema)) {
				fillBuffer();
				// row.clear();
				row = buffer.pollFirst();
				return true;
			}

			EqualsTo equalsToExpression = (EqualsTo) joinCondition;
			Expression e = new GreaterThan(equalsToExpression.getLeftExpression(),
					equalsToExpression.getRightExpression());

			if (utils.filterRow(row, e, fromSchema)) {
				if (++rightBufferIndex >= rightBuffer.size()) {
					row = null;
					return false;
				}
			} else {
				if (++leftBufferIndex >= leftBuffer.size()) {
					row = null;
					return false;
				}
			}
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
