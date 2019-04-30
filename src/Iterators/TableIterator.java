package Iterators;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
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
				BufferedReader br = new BufferedReader(
						new FileReader(TEMP_DIR + "ColStore/" + table.getName() + "/" + columnName));

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
		try {
			int i = 0;
			String line = "";
			ArrayList<PrimitiveValue> tmp = new ArrayList<PrimitiveValue>();
			for (BufferedReader br : readers) {
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
			}

			return tmp;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public void setIteratorSchema() {
		fromSchema = new TupleSchema();

		int i = 0;
		for (String columnName : columns) {
			Schema s = Main.tableSchemas.get(table.getName()).getSchemaByName(columnName);
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
