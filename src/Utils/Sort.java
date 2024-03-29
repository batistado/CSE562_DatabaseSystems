package Utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

import Iterators.RAIterator;
import Models.TupleSchema;
import dubstep.Main;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class Sort {
	public static String getColumnName(Column column) {
		String columnTableName = column.getTable().getName();
		return columnTableName == null ? column.getColumnName() : columnTableName + "." + column.getColumnName();
	}
	
	public int sortComparator(ArrayList<PrimitiveValue> a, ArrayList<PrimitiveValue> b, List<OrderByElement> orderByElements, TupleSchema fromSchema) {
		// TODO Auto-generated method stub
			int c = 0;
			for (OrderByElement o : orderByElements) {
				boolean isAscending = o.isAsc();

				PrimitiveValue pa = a.get(fromSchema.getSchemaByName(getColumnName((Column) o.getExpression())).getColumnIndex());
				PrimitiveValue pb = b.get(fromSchema.getSchemaByName(getColumnName((Column) o.getExpression())).getColumnIndex());
				//PrimitiveValue pa = utils.projectColumnValue(a, o.getExpression(), fromSchema);
				//PrimitiveValue pb = utils.projectColumnValue(b, o.getExpression(), fromSchema);
				try {
					if (pa instanceof LongValue && pb instanceof LongValue) {
						if (pa.toLong() > pb.toLong()) {
							c = 1;
						}
						if (pa.toLong() < pb.toLong()) {
							c = -1;
						}
					} else if (pa instanceof DoubleValue && pb instanceof DoubleValue) {
						c = Double.compare(pa.toDouble(), pb.toDouble());
					} else if (pa instanceof DateValue && pb instanceof DateValue) {
						DateValue dpa = (DateValue) pa;
						DateValue dpb = (DateValue) pb;

						if ((dpa.getYear() * 10000 + dpa.getMonth() * 100
								+ dpa.getDate()) > (dpb.getYear() * 10000 + dpb.getMonth() * 100
										+ dpb.getDate())) {
							c = 1;
						}
						if ((dpa.getYear() * 10000 + dpa.getMonth() * 100
								+ dpa.getDate()) < (dpb.getYear() * 10000 + dpb.getMonth() * 100
										+ dpb.getDate())) {
							c = -1;
						}
					} else if (pa instanceof StringValue && pb instanceof StringValue) {
						c = pa.toString().compareTo(pb.toString());
					} else {
						c = pa.toString().compareTo(pb.toString());
					}

					if (c != 0) {
						c = isAscending ? c : -1 * c;
						break;
					}
				} catch (InvalidPrimitive i) {
					i.printStackTrace();
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				}
			}
			return c;
	}

	public String sortData(RAIterator rightIterator, List<OrderByElement> orderByElements, TupleSchema fromSchema,
			String directory, ArrayList<ArrayList<PrimitiveValue>> buffer, boolean isInMemory) {
		if (isInMemory) {
			while (rightIterator.hasNext()) {
				buffer.add(rightIterator.next());
			}
			sort(buffer, orderByElements, fromSchema);
			
			return null;
		}
		
		ArrayList<String> tempFiles = new ArrayList<String>();
		ArrayList<ArrayList<PrimitiveValue>> sortedRows = new ArrayList<ArrayList<PrimitiveValue>>();
		int count = 0;
		while (rightIterator.hasNext()) {
			count++;
			sortedRows.add(rightIterator.next());
			if (count == Main.sortBufferSize) {
				count = 0;
				sort(sortedRows, orderByElements, fromSchema);

				String tempFile = writeBuffer(sortedRows);

				if (tempFile != null)
					tempFiles.add(tempFile);

				
//				sortedRows = null;
				sortedRows = new ArrayList<ArrayList<PrimitiveValue>>();
				//System.gc();
			}
		}

		if (!sortedRows.isEmpty()) {
			sort(sortedRows, orderByElements, fromSchema);

			String tempFile = writeBuffer(sortedRows);

			if (tempFile != null)
				tempFiles.add(tempFile);
			sortedRows = new ArrayList<ArrayList<PrimitiveValue>>();
			//System.gc();
		}
		//System.gc();
		//return null;
		return mergeFiles(tempFiles, orderByElements, fromSchema);

	}

	public void sort(ArrayList<ArrayList<PrimitiveValue>> buffer, List<OrderByElement> orderByElements, TupleSchema fromSchema) {
		try {
			Collections.sort(buffer, new Comparator<ArrayList<PrimitiveValue>>() {
				@Override
				public int compare(ArrayList<PrimitiveValue> o1, ArrayList<PrimitiveValue> o2) {
					// TODO Auto-generated method stub
					return sortComparator(o1, o2, orderByElements, fromSchema);
				}
			});
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
	}

	public String writeBuffer(ArrayList<ArrayList<PrimitiveValue>> sortedRows) {
		try {
			File temp = File.createTempFile("Temp", ".csv", new File(RAIterator.TEMP_DIR));
			BufferedWriter bw = new BufferedWriter(new FileWriter(temp));
			for (ArrayList<PrimitiveValue> i : sortedRows) {
				bw.write(utils.getOutputString(i));
				//bw.write(i.toString());
				bw.write("\n");
				i = null;
			}
			sortedRows = null;
			bw.flush();
			bw.close();
			return temp.getAbsolutePath();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	public String mergeFiles(ArrayList<String> tempFiles, List<OrderByElement> orderByElements, TupleSchema fromSchema) {
		PriorityQueue<BrIterator> pq = new PriorityQueue<BrIterator>(createComparator(orderByElements, fromSchema));
		LinkedList<String> queue = new LinkedList<String>();

		//System.out.println(tempFiles.size());
		for (String tempFileI : tempFiles) {
			queue.add(tempFileI);
		}

		tempFiles.clear();
		tempFiles = null;

		while (queue.size() > 1) {
			int count = 0;

			while (count < Main.sortedRunSize && !queue.isEmpty()) {
				String tempFilePath = queue.pollFirst();
				BrIterator br = new BrIterator(tempFilePath);
				if (br.hasNext()) {
					pq.add(br);
				} else {
					br = null;
				}
				count++;
			}

			try {
				File temp = File.createTempFile("Temp", ".csv", new File(RAIterator.TEMP_DIR));
				BufferedWriter bw = new BufferedWriter(new FileWriter(temp));
				while (!pq.isEmpty()) {
					BrIterator it = pq.poll();
					bw.write(it.next());
					bw.write("\n");
					if (it.hasNext()) {
						pq.add(it);
					} else {
						it = null;
					}
				}
				bw.flush();
				bw.close();
				queue.add(temp.getAbsolutePath());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		System.gc();
		if (queue.size() == 0) {
			try {
				File temp = File.createTempFile("Temp", ".csv", new File(RAIterator.TEMP_DIR));
				return temp.getAbsolutePath();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;

		}

		
		return queue.get(0);
	}

	public Comparator<BrIterator> createComparator(List<OrderByElement> orderByElements, TupleSchema fromSchema) {
		return new Comparator<BrIterator>() {

			@Override
			public int compare(BrIterator c1, BrIterator c2) {
				return sortComparator(getRow(c1.next()), getRow(c2.next()), orderByElements, fromSchema);
			}

			public ArrayList<PrimitiveValue> getRow(String unSplittedLine) {
				int i = 0;
				ArrayList<PrimitiveValue> tmp = new ArrayList<PrimitiveValue>();
				for (String word : unSplittedLine.split("\\|")) {
					Integer colDatatype = fromSchema.getSchemaByIndex(i).getDataType();
					if(colDatatype == 1) {
						StringValue val = new StringValue(word);
						tmp.add(val);
					}
					else if(colDatatype == 2){
						LongValue val = new LongValue(word);
						tmp.add(val);
					}
					else if(colDatatype == 3) {
						DoubleValue val = new DoubleValue(word);
						tmp.add(val);
					}
					else if(colDatatype == 4){
						DateValue val = new DateValue(word);
						tmp.add(val);
					}
					i++;
				}
				return tmp;
			}
		};
	}
}

class BrIterator implements Iterator<String> {

	BufferedReader br = null;
	String st;

	BrIterator(String filename) {
		try {
			br = new BufferedReader(new FileReader(filename));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public String next() {
		// TODO Auto-generated method stub
		return st;
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		try {
			st = null;
			if ((st = br.readLine()) != null) {
				return true;
			} else {
				br.close();
				return false;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
}
