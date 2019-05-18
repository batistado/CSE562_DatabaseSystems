package Iterators;
import java.util.ArrayList;
import java.util.List;

import Models.TupleSchema;
import dubstep.Main;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectItem;

public class InsertIterator implements RAIterator{
	private ArrayList<ArrayList<PrimitiveValue>> rows;
	private Table table;
	private TupleSchema fromSchema;
	private int index;
	
	public InsertIterator (Table table) {
		this.table = table;
		initializeReader();
		this.setIteratorSchema();
		//System.gc();
	}
	
	public Table getTable() {
		return this.table;
	}
	
	@Override
	public void resetProjection() {
	}
	
	public void resetIterator() {
		initializeReader();
	}
	
	public void initializeReader() {
		index = -1;
		rows = Main.inserts.get(table.getName());
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		index++;
		if (index >= rows.size())
			return false;
		
		return true;
	}
	
	public void setIteratorSchema() {
		fromSchema = Main.tableSchemas.get(table.getName());
	}

	@Override
	public ArrayList<PrimitiveValue> next() {
		// TODO Auto-generated method stub
		return rows.get(index);
	}

	@Override
	public TupleSchema getIteratorSchema() {
		// TODO Auto-generated method stub
		return fromSchema;
	}

	@Override
	public void addSelectItems(List<SelectItem> selectItems) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public TupleSchema getSelectSchema() {
		// TODO Auto-generated method stub
		return fromSchema;
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

	@Override
	public void resetWhere() {
		// TODO Auto-generated method stub
		
	}
}
