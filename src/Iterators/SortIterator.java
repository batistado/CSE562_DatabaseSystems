package Iterators;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import Models.Schema;
import Models.TupleSchema;
import Utils.Sort;
import dubstep.Main;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectItem;

public class SortIterator implements RAIterator{
	private RAIterator rightIterator = null;
	private BufferedReader reader;
	private ArrayList<PrimitiveValue> row;
	private String line;
	private TupleSchema fromSchema;
	private String fileName;
	private ArrayList<ArrayList<PrimitiveValue>> buffer = null;
	private Integer bufferIndex;
	
	public SortIterator (RAIterator rightIterator, List<OrderByElement> orderByElements) {
		this.rightIterator = rightIterator;
		this.setIteratorSchema();
		this.buffer = new ArrayList<ArrayList<PrimitiveValue>>();
		
		Sort s = new Sort(rightIterator, orderByElements, fromSchema, DIR, buffer);
		fileName = s.sortData();
		initializeReader();
		System.gc();
	}
	
	private void initializeReader() {
		// TODO Auto-generated method stub
		try {
			if (Main.isInMemory) {
				bufferIndex = -1;
			} else {
				buffer.clear();
				reader = new BufferedReader(new FileReader(fileName));
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void resetIterator() {
		initializeReader();
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		try {
			if (Main.isInMemory) {
				if (++bufferIndex >= buffer.size()) {
					row = null;
					return false;
				}
				
				row = buffer.get(bufferIndex);
				return true;
			}
			
			while (reader != null && (line = reader.readLine()) != null) {
				row = getLeftRow();
				return true;
			}
			
			if (reader != null) {
				row = null;
				reader.close();
				reader = null;
			}
			return false;
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	private ArrayList<PrimitiveValue> getLeftRow() {
		// TODO Auto-generated method stub
		String[] row = line.split("\\|");
		int j = 0;
		ArrayList<PrimitiveValue> tmp = new ArrayList<PrimitiveValue>();
		for(String x : row) {
			String colDatatype = fromSchema.getSchemaByIndex(j).getDataType();
			if(colDatatype.equals("string") || colDatatype.equals("varchar") || colDatatype.equals("char")) {
				StringValue val = new StringValue(x);
				tmp.add(val);
			}
			else if(colDatatype.equals("int")){
				LongValue val = new LongValue(x);
				tmp.add(val);
			}
			else if(colDatatype.equals("decimal")) {
				DoubleValue val = new DoubleValue(x);
				tmp.add(val);
			}
			else if(colDatatype.equals("date")){
				DateValue val = new DateValue(x);
				tmp.add(val);
			}
			j++;
		}
		
		return tmp;
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

