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
import net.sf.jsqlparser.statement.select.OrderByElement;

public class Sort {
	private ArrayList<File> tempFiles = new ArrayList<File>();
	private List<OrderByElement> orderByElements;
	private String outputFile;
	private RAIterator rightIterator = null;
	private ArrayList<ArrayList<PrimitiveValue>> rows;
	private Comparator<BrIterator> customComparator = null;
	private TupleSchema fromSchema;
	private String directory;

	public Sort(RAIterator rightIterator, List<OrderByElement> orderByElements, TupleSchema fromSchema,
			String directory, ArrayList<ArrayList<PrimitiveValue>> buffer) {
		this.rightIterator = rightIterator;
		this.orderByElements = orderByElements;
		this.fromSchema = fromSchema;
		this.directory = directory;

		if (!Main.isInMemory) {
			this.rows = new ArrayList<ArrayList<PrimitiveValue>>();
			buffer = null;
		} else {
			this.rows = buffer;
		}
	}

	public int sortComparator(ArrayList<PrimitiveValue> a, ArrayList<PrimitiveValue> b) {
		// TODO Auto-generated method stub
		int c = 0;
		for (OrderByElement o : orderByElements) {
			boolean isAscending = o.isAsc();

			PrimitiveValue pa = utils.projectColumnValue(a, o.getExpression(), fromSchema);
			PrimitiveValue pb = utils.projectColumnValue(b, o.getExpression(), fromSchema);
			try {
				if (pa instanceof LongValue && pb instanceof LongValue) {
					if (pa.toLong() > pb.toLong()) {
						c = 1;
					}
					if (pa.toLong() < pb.toLong()) {
						c = -1;
					}
				} else if (pa instanceof DoubleValue && pb instanceof DoubleValue) {
					if (pa.toDouble() > pb.toDouble()) {
						c = 1;
					}
					if (pa.toDouble() < pa.toDouble()) {
						c = -1;
					}
				} else if (pa instanceof DateValue && pb instanceof DateValue) {
					DateValue dpa = (DateValue) pa;
					DateValue dpb = (DateValue) pb;

					if ((dpa.getYear() * 10000 + dpa.getMonth() * 100 + dpa.getDate()) > (dpb.getYear() * 10000
							+ dpb.getMonth() * 100 + dpb.getDate())) {
						c = 1;
					}
					if ((dpa.getYear() * 10000 + dpa.getMonth() * 100 + dpa.getDate()) < (dpb.getYear() * 10000
							+ dpb.getMonth() * 100 + dpb.getDate())) {
						c = -1;
					}
				} else {
					c = pa.toString().compareTo(pb.toString());
				}

				if (c != 0) {
					c = isAscending ? c : -1 * c;
					break;
				}
			} catch (InvalidPrimitive i) {
				i.printStackTrace();
			}
		}
		return c;
	}

	public String sortData() {
		if (Main.isInMemory)
			return sortInMemory();

		readData();
		this.customComparator = createComparator();
		mergeFiles();
		return outputFile;
	}

	public String sortInMemory() {
		while (rightIterator.hasNext()) {
			rows.add(rightIterator.next());
		}

		sort();
		return null;
	}

	public void readData() {
		int count = 0;
		while (rightIterator.hasNext()) {
			count++;
			rows.add(rightIterator.next());
			if (count == 50000) {
				count = 0;
				sort();

				File tempFile = writeBuffer();

				if (tempFile != null)
					tempFiles.add(tempFile);

				rows = new ArrayList<ArrayList<PrimitiveValue>>();
			}
		}

		if (!rows.isEmpty()) {
			sort();

			File tempFile = writeBuffer();

			if (tempFile != null)
				tempFiles.add(tempFile);
			rows = new ArrayList<ArrayList<PrimitiveValue>>();
		}
	}

	public void sort() {
		Collections.sort(rows, new Comparator<ArrayList<PrimitiveValue>>() {
			@Override
			public int compare(ArrayList<PrimitiveValue> a, ArrayList<PrimitiveValue> b) {
				return sortComparator(a, b);
			}
		});
	}

	public File writeBuffer() {
		try {
			File temp = File.createTempFile("Temp", ".csv", new File(directory));
			BufferedWriter bw = new BufferedWriter(new FileWriter(temp));
			for (ArrayList<PrimitiveValue> i : rows) {
				bw.write(utils.getOutputString(i));
				bw.write("\n");
			}
			bw.close();
			return temp;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	public void mergeFiles() {
		PriorityQueue<BrIterator> pq = new PriorityQueue<BrIterator>(customComparator);
		LinkedList<String> queue = new LinkedList<String>();

		for (File tempFileI : tempFiles) {
			queue.add(tempFileI.getAbsolutePath());
		}

		while (queue.size() > 1) {
			int count = 0;

			while (count < Main.sortedRunSize && !queue.isEmpty()) {
				String tempFilePath = queue.pollFirst();
				BrIterator br = new BrIterator(tempFilePath);
				if (br.hasNext()) {
					pq.add(br);
				}
				count++;
			}

			try {
				File temp = File.createTempFile("Temp", ".csv", new File(directory));
				BufferedWriter bw = new BufferedWriter(new FileWriter(temp));
				while (!pq.isEmpty()) {
					BrIterator it = pq.poll();
					bw.write(it.next());
					bw.write("\n");
					if (it.hasNext()) {
						pq.add(it);
					}
				}
				bw.close();
				queue.add(temp.getAbsolutePath());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		outputFile = queue.get(0);
	}

	public Comparator<BrIterator> createComparator() {
		return new Comparator<BrIterator>() {

			@Override
			public int compare(BrIterator c1, BrIterator c2) {
				// TODO Auto-generated method stub
				String line1[] = c1.next().split("\\|");
				String line2[] = c2.next().split("\\|");

				ArrayList<PrimitiveValue> tmp1 = getRow(line1);
				ArrayList<PrimitiveValue> tmp2 = getRow(line2);

				return sortComparator(tmp1, tmp2);
			}

			public ArrayList<PrimitiveValue> getRow(String[] line) {
				int i = 0;
				ArrayList<PrimitiveValue> tmp = new ArrayList<PrimitiveValue>();
				for (String word : line) {
					String colDatatype = fromSchema.getSchemaByIndex(i).getDataType();
					if (colDatatype.equals("string") || colDatatype.equals("varchar") || colDatatype.equals("char")) {
						StringValue val = new StringValue(word);
						tmp.add(val);
					} else if (colDatatype.equals("int")) {
						LongValue val = new LongValue(word);
						tmp.add(val);
					} else if (colDatatype.equals("decimal")) {
						DoubleValue val = new DoubleValue(word);
						tmp.add(val);
					} else if (colDatatype.equals("date")) {
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
			if ((st = br.readLine()) != null) {
				return true;
			} else {
				br.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return false;
	}
}
