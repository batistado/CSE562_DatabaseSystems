package Iterators;

import java.util.ArrayList;
import java.util.List;

import Models.TupleSchema;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.SelectItem;

public class UnionIterator implements RAIterator {
	private List<RAIterator> iterators = null;
	private ArrayList<PrimitiveValue> row;
	private Integer currentIteratorIndex;
	
	public UnionIterator(List<RAIterator> iterators) {
		this.iterators = iterators;
		currentIteratorIndex = 0;
		//System.gc();
	}
	
	public void pushDownSchema(List<RAIterator> iterators) {
		this.iterators = iterators;
	}

	@Override
	public void resetWhere() {
		// TODO Auto-generated method stub
		
	}
	
	public List<RAIterator> getIterators(){
		return this.iterators;
	}

	@Override
	public TupleSchema getIteratorSchema() {
		// TODO Auto-generated method stub
		return iterators.get(0).getIteratorSchema();
	}

	@Override
	public void resetIterator() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void resetProjection() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ArrayList<PrimitiveValue> next() {
		// TODO Auto-generated method stub
		return row;
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		while (currentIteratorIndex < iterators.size()) {
			RAIterator iterator = iterators.get(currentIteratorIndex);
			
			if (iterator.hasNext()) {
				row = iterator.next();
				return true;
			}
			
			currentIteratorIndex++;
		}
		
		return false;
	}

	@Override
	public void addSelectItems(List<SelectItem> selectItems) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public TupleSchema getSelectSchema() {
		// TODO Auto-generated method stub
		return iterators.get(0).getSelectSchema();
	}
	
	@Override
	public RAIterator getLeftIterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RAIterator getRightIterator() {
		// TODO Auto-generated method stub
		return null;
	}
}
