package Iterators;
import java.util.ArrayList;
import java.util.List;
import Models.TupleSchema;
import Utils.utils;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.SelectItem;

public class SelectIterator implements RAIterator{
	private RAIterator rightIterator = null;
	private ArrayList<PrimitiveValue> row;
	private Expression where;
	private TupleSchema fromSchema;
	
	public SelectIterator (RAIterator rightIterator, Expression where) {
		this.rightIterator = rightIterator;
		this.where = where;
		this.setIteratorSchema();
		//System.gc();
	}
	
	public void pushDownSchema(RAIterator iterator) {
		this.rightIterator = iterator;
		
		setIteratorSchema();
	}
	
	public void resetIterator() {
		rightIterator.resetIterator();
	}

	public Expression getExpression() {
		return this.where;
	}
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		while (rightIterator.hasNext()) {
			row = rightIterator.next();
			if (where == null || utils.filterRow(row, where, fromSchema))
				return true;
		}
		
		return false;
	}
	
	public void setIteratorSchema() {
		if (rightIterator == null)
			return;
		
		fromSchema = rightIterator.getIteratorSchema();
	}

	@Override
	public ArrayList<PrimitiveValue> next() {
		// TODO Auto-generated method stub
		return row;
	}

	@Override
	public void resetWhere() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public TupleSchema getIteratorSchema() {
		// TODO Auto-generated method stub
		return fromSchema;
	}

	@Override
	public void resetProjection() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addSelectItems(List<SelectItem> selectItems) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public TupleSchema getSelectSchema() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public RAIterator getLeftIterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RAIterator getRightIterator() {
		// TODO Auto-generated method stub
		return rightIterator;
	}
}

