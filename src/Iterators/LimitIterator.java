package Iterators;
import java.util.ArrayList;
import java.util.List;
import Models.TupleSchema;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.SelectItem;

public class LimitIterator implements RAIterator{
	private RAIterator rightIterator = null;
	private ArrayList<PrimitiveValue> row;
	private TupleSchema fromSchema;
	private Limit limit;
	private Long currentCount = (long) 0;
	
	public LimitIterator(RAIterator rightIterator, Limit limit) {
		this.rightIterator = rightIterator;
		this.limit = limit;
		setIteratorSchema();
		//System.gc();
	}
	
	public Limit getLimit() {
		return this.limit;
	}
	
	public void resetIterator() {
		rightIterator.resetIterator();
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		if (!rightIterator.hasNext() || this.currentCount >= this.limit.getRowCount())
			return false;
		
		this.currentCount++;
		row = rightIterator.next();
		return true;
	}

	@Override
	public ArrayList<PrimitiveValue> next() {
		// TODO Auto-generated method stub
		return row;
	}
	
	public TupleSchema getIteratorSchema() {
		return fromSchema;
	}
	
	public void setIteratorSchema() {
		if (rightIterator == null)
			return;
		
		fromSchema = rightIterator.getIteratorSchema();
	}


	@Override
	public void resetWhere() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void resetProjection() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public TupleSchema getSelectSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addSelectItems(List<SelectItem> selectItems) {
		// TODO Auto-generated method stub
		
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
