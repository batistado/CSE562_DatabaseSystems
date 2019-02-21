package Iterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import dubstep.TupleSchema;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.SelectItem;

public interface RAIterator extends Iterator<ArrayList<PrimitiveValue>>  {
	//public final String DIR = "/Users/msyed3/Downloads/sample queries/Sanity_Check_Examples/data/";
	//public final String DIR = "/Users/msyed3/Downloads/sample queries/NBA_Examples/";
	public final String DIR = "/data/";
	
	public void resetWhere();
	
	public TupleSchema getIteratorSchema();
	
	public void resetIterator();
	
	public void resetProjection();
	
	public ArrayList<PrimitiveValue> next();
	
	public boolean hasNext();
	
	public void addSelectItems(List<SelectItem> selectItems);
	
	public TupleSchema getSelectSchema();
}