package Iterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import Models.TupleSchema;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.SelectItem;

public interface RAIterator extends Iterator<ArrayList<PrimitiveValue>>  {
	//public final String DIR = "/Users/msyed3/Downloads/sample queries/Sanity_Check_Examples/data/";
	//public final String DIR = "/Users/areeb/eclipse-workspace/team8/data/";
	//public final String DIR = "/Users/msyed3/Downloads/sample queries/NBA_Examples/";
	//public final String DIR = "/Users/msyed3/Downloads/sample queries/Test/50MB Data/";
	public final String DIR = "data/";
	
	public final String TEMP_DIR = "team8_temp/";
	
	public void resetWhere();
	
	public TupleSchema getIteratorSchema();
	
	public void resetIterator();
	
	public void resetProjection();
	
	public ArrayList<PrimitiveValue> next();
	
	public boolean hasNext();
	
	public void addSelectItems(List<SelectItem> selectItems);
	
	public TupleSchema getSelectSchema();
	
	public RAIterator getLeftIterator();
	
	public RAIterator getRightIterator();
}
