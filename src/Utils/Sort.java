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
import java.util.List;
import java.util.PriorityQueue;

import Iterators.RAIterator;
import Models.TupleSchema;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class Sort {
	private ArrayList<String> tempFiles = new ArrayList<String>();
	private PriorityQueue<BrIterator> pq = null;
	private List<OrderByElement> orderByElement;
	private String outputFile;
	private RAIterator rightIterator = null;
	private ArrayList<ArrayList<PrimitiveValue>> rows = new ArrayList<ArrayList<PrimitiveValue>>();
	private Comparator<BrIterator> customComparator = null;
	private TupleSchema fromSchema;
	
	public Sort(RAIterator rightIterator, List<OrderByElement> orderByElement, TupleSchema fromSchema){
		this.rightIterator = rightIterator;
		this.orderByElement = orderByElement;
		this.fromSchema = fromSchema;
	}
	
	public String sortData(){
		readData();
		this.customComparator = createComparator();
		mergeFiles();
		return outputFile;
	}
	
	public void readData() {
		int count = 0;
		while(rightIterator.hasNext()) {
			count++;
			rows.add(rightIterator.next());
			if(count == 500) {
				count = 0;
				sort();
				writeBuffer();
				rows = new ArrayList<ArrayList<PrimitiveValue>>();
			}
		}
		
		if(!rows.isEmpty()) {
			sort();
			writeBuffer();
			rows = new ArrayList<ArrayList<PrimitiveValue>>();
		}
	}
	
	public void sort() {
		Collections.sort(rows, new Comparator<ArrayList<PrimitiveValue>>() {
			@Override
			public int compare(ArrayList<PrimitiveValue> a, ArrayList<PrimitiveValue> b) {
				// TODO Auto-generated method stub
				int c = 0;
				for(OrderByElement o: orderByElement) {
					PrimitiveValue pa = utils.projectColumnValue(a, o.getExpression(), fromSchema);
					PrimitiveValue pb = utils.projectColumnValue(b, o.getExpression(), fromSchema);
					try {
						if(pa instanceof LongValue && pb instanceof LongValue) {
							if(pa.toLong() > pb.toLong()) {
								c = 1;
							}
							if(pa.toLong() < pb.toLong()) {
								c = -1;
							}
						} else if(pa instanceof DoubleValue && pb instanceof DoubleValue) {
							if(pa.toDouble() > pb.toDouble()) {
								c = 1;
							}
							if(pa.toDouble() < pa.toDouble()) {
								c = -1;
							}
						}else {
							c = pa.toString().compareTo(pb.toString());
						}
						
						if(c != 0) {
							break;
						}
					}catch(InvalidPrimitive i) {
						i.printStackTrace();
					}
				}
				return c;
			}
		});
	}
	
	public void writeBuffer() {
		try {
			File temp = File.createTempFile("Temp", ".csv");
			temp.deleteOnExit();
			BufferedWriter bw = new BufferedWriter(new FileWriter(temp));
			for(ArrayList<PrimitiveValue> i: rows) {
				StringBuffer tmp = new StringBuffer();
				for(PrimitiveValue j: i) {
					tmp.append(j.toString());
					tmp.append("|");
				}
				bw.write(tmp.substring(0, tmp.length()-1));
				bw.write("\n");
			}
			bw.close();
			tempFiles.add(temp.getName());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void mergeFiles() {
		pq = new PriorityQueue<BrIterator>(customComparator);
		String path = System.getProperty("java.io.tmpdir");
		StringBuffer filePath;
		for(String i: tempFiles) {
			filePath = new StringBuffer(path);
			filePath.append(i);
			BrIterator br = new BrIterator(filePath.toString());
			if(br.hasNext()) {
				pq.add(br);
			}
		}
		
		File temp;
		try {
			temp = File.createTempFile("TempFinalX", ".csv");
			BufferedWriter bw = new BufferedWriter(new FileWriter(temp));
			while(!pq.isEmpty()) {
				BrIterator it = pq.poll();
				bw.write(it.next());
				bw.write("\n");
				if(it.hasNext()) {
					pq.add(it);
				}
			}
			outputFile = temp.getName();
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
				
				int c = 0;
				for(OrderByElement o: orderByElement) {
					PrimitiveValue pa = utils.projectColumnValue(tmp1, o.getExpression(), fromSchema);
					PrimitiveValue pb = utils.projectColumnValue(tmp2, o.getExpression(), fromSchema);
					try {
						if(pa instanceof LongValue && pb instanceof LongValue) {
							if(pa.toLong() > pb.toLong()) {
								c = 1;
							}
							if(pa.toLong() < pb.toLong()) {
								c = -1;
							}
						} else if(pa instanceof DoubleValue && pb instanceof DoubleValue) {
							if(pa.toDouble() > pb.toDouble()) {
								c = 1;
							}
							if(pa.toDouble() < pa.toDouble()) {
								c = -1;
							}
						} else if(pa instanceof DateValue && pb instanceof DateValue){
							DateValue dpa = (DateValue) pa;
							DateValue dpb = (DateValue) pb;
							
							if((dpa.getYear()*10000+dpa.getMonth()*100+dpa.getDate()) > (dpb.getYear()*10000+dpb.getMonth()*100+dpb.getDate())) {
								c = 1;
							}
							if((dpa.getYear()*10000+dpa.getMonth()*100+dpa.getDate()) < (dpb.getYear()*10000+dpb.getMonth()*100+dpb.getDate())) {
								c = -1;
							}
						} else {
							c = pa.toString().compareTo(pb.toString());
						}
						
						if(c != 0) {
							break;
						}
					}catch(InvalidPrimitive i) {
						i.printStackTrace();
					}
				}
				return c;
			}
			
			public ArrayList<PrimitiveValue> getRow(String[] line){
				int i = 0;
				ArrayList<PrimitiveValue> tmp = new ArrayList<PrimitiveValue>();
				for(String word : line) {
					String colDatatype = fromSchema.getSchemaByIndex(i).getDataType();
					if(colDatatype.equals("string") || colDatatype.equals("varchar") || colDatatype.equals("char")) {
						StringValue val = new StringValue(word);
						tmp.add(val);
					}
					else if(colDatatype.equals("int")){
						LongValue val = new LongValue(word);
						tmp.add(val);
					}
					else if(colDatatype.equals("decimal")) {
						DoubleValue val = new DoubleValue(word);
						tmp.add(val);
					}
					else if(colDatatype.equals("date")){
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

class BrIterator implements Iterator<String>{

	BufferedReader br = null;
	String st;
	BrIterator(String filename){
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
			if((st = br.readLine()) != null) {
				return true;
			}else {
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
