package Iterators;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import Models.Schema;
import Models.TupleSchema;
import dubstep.Main;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectItem;

public class TableIterator implements RAIterator {
	private ArrayList<PrimitiveValue> row;
	private Table table;
	private TupleSchema fromSchema;
	private List<String> columns;
	private BufferedReader[] readers;

	public TableIterator(Table table, List<String> columns) {
		this.table = table;
		this.columns = columns;
		initializeReaders();
		this.setIteratorSchema();
	}

	public Table getTable() {
		return table;
	}

	@Override
	public void resetWhere() {

	}

	@Override
	public void resetProjection() {
	}

	public void resetIterator() {
		closeReaders();
		initializeReaders();
	}

	public void closeReaders() {
		for (BufferedReader br : readers) {
			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void initializeReaders() {
		try {
			readers = new BufferedReader[columns.size()];

			int i = 0;
			for (String columnName : columns) {
				BufferedReader br = null;
				if (columnName.lastIndexOf('.') != -1) {
				br = new BufferedReader(
						new FileReader(TEMP_DIR + "ColStore/" + table.getName() + "/" + columnName));
				} else {
				br = new BufferedReader(
							new FileReader(TEMP_DIR + "ColStore/" + table.getName() + "/" + table.getName() + "." + columnName));
				}

				readers[i] = br;
				i++;
			}

		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		if ((row = getRow()) != null) {
			return true;
		}

		closeReaders();
		return false;
	}

	public ArrayList<PrimitiveValue> getRow() {
			int i = 0;
			String line;
			ArrayList<PrimitiveValue> tmp = new ArrayList<PrimitiveValue>();
//			PrimitiveValue[] arr = new PrimitiveValue[columns.size()];
//			List<Thread> threads = new ArrayList<Thread>();
			for (BufferedReader br : readers) {
//				int index = i;
//				Thread thread = new Thread(new Runnable() {
//				    @Override
//				    public void run() {
//				        readRow(br, index, arr);
//				    }
//				});
//				threads.add(thread);
//				thread.start();
//				i++;
		
				try {
					if ((line = br.readLine()) != null) {
						int colDatatype = fromSchema.getSchemaByIndex(i).getDataType();
						if (colDatatype == 1) {
							StringValue val = new StringValue(line);
							tmp.add(val);
						} else if (colDatatype == 2) {
							LongValue val = new LongValue(line);
							tmp.add(val);
						} else if (colDatatype == 3) {
							DoubleValue val = new DoubleValue(line);
							tmp.add(val);
						} else if (colDatatype == 4) {
							DateValue val = new DateValue(line);
							tmp.add(val);
						}
						i++;
					} else {
						return null;
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
//			
//			for (Thread thread: threads) {
//				try {
//					thread.join();
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
//			
//			//System.out.println(arr.toString());
//			if (arr[0] != null)
//				tmp.addAll(Arrays.asList(arr));
//			else
//				return null;

			return tmp;
	}
	
	public void readRow(BufferedReader reader, int index, PrimitiveValue[] arr) {
		String line;
		try {
			if ((line = reader.readLine()) != null) {
				int colDatatype = fromSchema.getSchemaByIndex(index).getDataType();
				if (colDatatype == 1) {
					arr[index] = new StringValue(line);
				} else if (colDatatype == 2) {
					arr[index] = new LongValue(line);
				} else if (colDatatype == 3) {
					arr[index] = new DoubleValue(line);
				} else if (colDatatype == 4) {
					arr[index] = new DateValue(line);
				} else {
					arr[index] = null;
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void setIteratorSchema() {
		fromSchema = new TupleSchema();

		int i = 0;
		for (String columnName : columns) {
			String col = columnName.lastIndexOf('.') != -1 ? columnName : table.getName() + "." + columnName;
			Schema s = Main.tableSchemas.get(table.getName()).getSchemaByName(col);
			fromSchema.addTuple(columnName, i, s.getDataType());
			i++;
		}
	}

	@Override
	public ArrayList<PrimitiveValue> next() {
		// TODO Auto-generated method stub
		return row;
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
		return null;
	}
}
