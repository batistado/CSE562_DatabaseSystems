package Indexes;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import Iterators.RAIterator;
import Models.TupleSchema;
import Utils.utils;
import dubstep.Main;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class BPlusTree {
	Comparator<PrimitiveValue> c = new Comparator<PrimitiveValue>() {
		@Override
		public int compare(PrimitiveValue o1, PrimitiveValue o2) {
			// TODO Auto-generated method stub
			return utils.primitiveValueComparator(o1, o2);
		}
	};

	public static enum RangePolicy {
		EXCLUSIVE, INCLUSIVE
	}

	private static final int DEFAULT_BRANCHING_FACTOR = 50;
	private int branchingFactor;

	private Node root;
	private Table table;
	private List<Column> indexOnElements;
	private List<OrderByElement> orderByElements;
	private boolean isSorted = false;
	private int totalBufferSize;

	public BPlusTree(Table table, List<Column> indexOnElements, boolean isSorted) {
		this(DEFAULT_BRANCHING_FACTOR, table, indexOnElements, isSorted);
	}

	public BPlusTree(int branchingFactor, Table table, List<Column> indexOnElements, boolean isSorted) {
		if (branchingFactor <= 2)
			throw new IllegalArgumentException("Illegal branching factor: " + branchingFactor);
		this.branchingFactor = branchingFactor;
		root = new LeafNode();
		this.table = table;
		this.indexOnElements = indexOnElements;
		this.totalBufferSize = 0;
		this.isSorted = isSorted;

		setIndexOnElements();
	}

	public void closeIndex() {
		LeafNode head = (LeafNode) root.getFirstLeafNode();

		while (head.next != null) {
			head = head.next;
		}
		
		head.dumpToDisk();
	}

	private void setIndexOnElements() {
		orderByElements = new ArrayList<OrderByElement>();
		for (Column column : indexOnElements) {
			OrderByElement o = new OrderByElement();
			o.setExpression(column);
			o.setAsc(true);
			orderByElements.add(o);
		}
	}

	public String search(PrimitiveValue key) {
		return root.getValue(key);
	}

	public List<String> searchRange(PrimitiveValue key1, RangePolicy policy1, PrimitiveValue key2,
			RangePolicy policy2) {
		return root.getRange(key1, policy1, key2, policy2);
	}

//	private BinaryExpression getGreaterThanExpression(Expression expression, Expression type) {
//		BinaryExpression binaryExpression = (BinaryExpression) expression;
//		if ((binaryExpression instanceof GreaterThan || binaryExpression instanceof GreaterThanEquals) && ((binaryExpression.getLeftExpression() instanceof Column) || (binaryExpression.getRightExpression() instanceof Column))) {
//			Column column = (Column) (binaryExpression.getLeftExpression() instanceof Column ? ((BinaryExpression) binaryExpression).getLeftExpression(): ((BinaryExpression) binaryExpression).getRightExpression());
//			
//			if (utils.getColumnName(column).equals(utils.getColumnName(primaryColumn))) {
//				return (BinaryExpression) binaryExpression;
//			} else {
//				return null;
//			}
//		}
//		
//		BinaryExpression left = getPrimaryExpression(((BinaryExpression) binaryExpression).getLeftExpression());
//		BinaryExpression right = getPrimaryExpression(((BinaryExpression) binaryExpression).getRightExpression());
//		return  left != null ? left : right;
//	}

	public void insert(ArrayList<PrimitiveValue> value) {
		PrimitiveValue key = utils.projectColumnValue(value, indexOnElements.get(0),
				Main.tableSchemas.get(utils.getTableName(table)));

		root = root.insertValue(key, value);
	}
	
	public String toString() {
		Queue<List<Node>> queue = new LinkedList<List<Node>>();
		queue.add(Arrays.asList(root));
		StringBuilder sb = new StringBuilder();
		while (!queue.isEmpty()) {
			Queue<List<Node>> nextQueue = new LinkedList<List<Node>>();
			while (!queue.isEmpty()) {
				List<Node> nodes = queue.remove();
				sb.append('{');
				Iterator<Node> it = nodes.iterator();
				while (it.hasNext()) {
					Node node = it.next();
					sb.append(node.toString());
					if (it.hasNext())
						sb.append(", ");
					if (node instanceof BPlusTree.InternalNode)
						nextQueue.add(((InternalNode) node).children);
				}
				sb.append('}');
				if (!queue.isEmpty())
					sb.append(", ");
				else
					sb.append('\n');
			}
			queue = nextQueue;
		}

		return sb.toString();
	}

	private class LeafNode extends Node {
		String fileName;
		ArrayList<ArrayList<PrimitiveValue>> buffer;
		LeafNode next;
		PrimitiveValue lastKey;

		LeafNode() {
			buffer = new ArrayList<ArrayList<PrimitiveValue>>();
			keys = new ArrayList<PrimitiveValue>();
			fileName = null;
			lastKey = null;
		}

		@Override
		String getValue(PrimitiveValue key) {
			return fileName;
		}

		@Override
		Node insertValue(PrimitiveValue key, ArrayList<PrimitiveValue> value) {
			int loc = Collections.binarySearch(keys, key, c);
			int valueIndex = loc >= 0 ? loc : -loc - 1;
			totalBufferSize++;

//			// Key already exists
//			if (loc >= 0) {
//				LinkedList<ArrayList<PrimitiveValue>> buffer = buffers.get(valueIndex);
//
//				buffer.add(value);
//
////				if (totalBufferSize > Main.sortBufferSize) {
////					totalBufferSize = 0;
////					closeIndex();
////				}
//			} else {
//				keys.add(valueIndex, key);
//				values.add(valueIndex, null);
//				LinkedList<ArrayList<PrimitiveValue>> buffer = new LinkedList<ArrayList<PrimitiveValue>>();
//				buffer.add(value);
//				buffers.add(valueIndex, buffer);
//			}
			if (loc < 0) {
				keys.add(valueIndex, key);
				lastKey = key;
			}
			
				
			if (buffer.size() > branchingFactor - 1 && key != lastKey) {
				Node sibling = split();
				((LeafNode) sibling).insertValue(key, value);
				InternalNode newRoot = new InternalNode();
				newRoot.keys.add(sibling.getFirstLeafKey());
				newRoot.children.add(this);
				newRoot.children.add(sibling);
				return newRoot;
				
			} else {
				buffer.add(value);
			}
			
			return this;
		}

//		void writeBuffersToDisk() {
//			for (int i = 0; i < values.size(); i++) {
//				values.set(i, writeToFile(buffers.get(i), values.get(i)));
//				//buffers.get(i).clear();
//				//System.out.println(buffers.get(i).size());
//			}
//		}

		public void sort(LinkedList<ArrayList<PrimitiveValue>> buffer, List<OrderByElement> orderByElements,
				TupleSchema fromSchema) {
			try {
				Collections.sort(buffer, new Comparator<ArrayList<PrimitiveValue>>() {
					@Override
					public int compare(ArrayList<PrimitiveValue> o1, ArrayList<PrimitiveValue> o2) {
						// TODO Auto-generated method stub
						return utils.sortComparator(o1, o2, orderByElements, fromSchema);
					}
				});
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			}
		}

		private String writeToFile(List<ArrayList<PrimitiveValue>> rows, String fileName) {
//			if (!isSorted)
//				return sortAndWriteToFile(rows, fileName);

			File temp = null;

			FileOutputStream fout;
			try {
				temp = File.createTempFile("Temp", ".csv", new File(RAIterator.TEMP_DIR));
				fout = new FileOutputStream(temp);
				ObjectOutputStream oos = new ObjectOutputStream(fout);

				if (fileName != null) {
					FileInputStream fis = new FileInputStream(RAIterator.TEMP_DIR + fileName);
					ObjectInputStream ois = new ObjectInputStream(fis);

					try {
						while (true) {
							oos.writeObject((ArrayList<PrimitiveValue>) ois.readObject());
							oos.reset();
						}

					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (EOFException eof) {
						ois.close();
					}
				}

				for (ArrayList<PrimitiveValue> row: rows) {
					oos.writeObject(row);
					oos.reset();
				}
				
				rows.clear();
				
				oos.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return temp.getName();

		}

		private String sortAndWriteToFile(LinkedList<ArrayList<PrimitiveValue>> rows, String fileName) {
			try {
				File temp = File.createTempFile("Temp", ".csv", new File(RAIterator.TEMP_DIR));
				FileOutputStream fout = new FileOutputStream(temp);
				ObjectOutputStream oos = new ObjectOutputStream(fout);

				if (fileName == null) {

					sort(rows, orderByElements, Main.tableSchemas.get(utils.getTableName(table)));
					while (rows.size() > 0) {
						oos.writeObject(rows.poll());
						oos.reset();
					}
					oos.close();
				} else {

					FileInputStream fis = new FileInputStream(RAIterator.TEMP_DIR + fileName);
					ObjectInputStream ois = new ObjectInputStream(fis);
					try {
						while (rows.size() > 0) {
							ArrayList<PrimitiveValue> streamRow = (ArrayList<PrimitiveValue>) ois.readObject();
							ArrayList<PrimitiveValue> bufferRow = rows.poll();

							if (utils.sortComparator(streamRow, bufferRow, orderByElements,
									Main.tableSchemas.get(utils.getTableName(table))) >= 0) {
								oos.writeObject(streamRow);
							} else {
								oos.writeObject(bufferRow);
							}
							oos.reset();

						}

					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (EOFException eof) {
						// Nothing
					}

					while (rows.size() > 0) {
						oos.writeObject(rows.poll());
						oos.reset();
					}

					try {
						while (true) {
							oos.writeObject((ArrayList<PrimitiveValue>) ois.readObject());
							oos.reset();
						}

					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (EOFException eof) {
						ois.close();
					}

					oos.close();

				}

				return temp.getName();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}

		@Override
		PrimitiveValue getFirstLeafKey() {
			return keys.get(0);
		}

		@Override
		Node getFirstLeafNode() {
			return this;
		}
		
		void dumpToDisk() {
			fileName = writeToFile(buffer, null);
		}

		@Override
		List<String> getRange(PrimitiveValue left, RangePolicy leftPolicy, PrimitiveValue right,
				RangePolicy rightPolicy) {
			return null;
//			List<String> result = new LinkedList<String>();
//
//			LeafNode node = this;
//			while (node != null) {
//				Iterator<PrimitiveValue> kIt = node.keys.iterator();
//				Iterator<String> vIt = node.values.iterator();
//				while (kIt.hasNext()) {
//					PrimitiveValue key = kIt.next();
//					String value = vIt.next();
//
//					Integer cmp1 = null;
//					Integer cmp2 = null;
//
//					if (left != null) {
//						cmp1 = utils.primitiveValueComparator(key, left);
//					}
//
//					if (right != null) {
//						cmp2 = utils.primitiveValueComparator(key, right);
//					}
//
//					if (cmp1 != null) {
//						if ((leftPolicy == RangePolicy.EXCLUSIVE && cmp1 > 0)
//								|| (leftPolicy == RangePolicy.INCLUSIVE && cmp1 >= 0)) {
//							if (cmp2 == null || (rightPolicy == RangePolicy.EXCLUSIVE && cmp2 < 0)
//									|| (rightPolicy == RangePolicy.INCLUSIVE && cmp2 <= 0)) {
//								result.add(value);
//							} else
//								return result;
//						}
//					} else {
//						if ((rightPolicy == RangePolicy.EXCLUSIVE && cmp2 < 0)
//								|| (rightPolicy == RangePolicy.INCLUSIVE && cmp2 <= 0)) {
//							result.add(value);
//						} else
//							return result;
//					}
//					return result;
//				}
//				node = node.next;
//			}
//			return result;
		}

		@Override
		Node split() {
			LeafNode sibling = new LeafNode();
//			int bufferSize = this.buffer.size();
//			
//			PrimitiveValue prevValue = utils.projectColumnValue(buffer.get(bufferSize / 2), indexOnElements.get(0),
//					Main.tableSchemas.get(utils.getTableName(table)));
//			
//			int i = bufferSize / 2 + 1;
//			PrimitiveValue currValue = utils.projectColumnValue(buffer.get(i), indexOnElements.get(0),
//					Main.tableSchemas.get(utils.getTableName(table)));
//			
//			while (utils.areEqual(prevValue, currValue) && i < buffer.size()) {
//				currValue = utils.projectColumnValue(buffer.get(bufferSize / 2), indexOnElements.get(0),
//						Main.tableSchemas.get(utils.getTableName(table)));
//				i++;
//			}
			
//			int from = 0, to = buffer.size() - 1;
			this.fileName = writeToFile(buffer, null);

			sibling.next = next;
			next = sibling;
			return sibling;
		}

		@Override
		boolean isOverflow() {
			return this.buffer.size() > branchingFactor - 1;
		}

		@Override
		boolean isUnderflow() {
			return false;
		}
	}

	public abstract class Node {
		List<PrimitiveValue> keys;

		int keyNumber() {
			return keys.size();
		}

		abstract String getValue(PrimitiveValue key);

		abstract Node getFirstLeafNode();

		abstract Node insertValue(PrimitiveValue key, ArrayList<PrimitiveValue> value);

		abstract PrimitiveValue getFirstLeafKey();

		abstract List<String> getRange(PrimitiveValue key1, RangePolicy policy1, PrimitiveValue key2,
				RangePolicy policy2);

		abstract Node split();

		abstract boolean isOverflow();

		abstract boolean isUnderflow();

		public String toString() {
			return keys.toString();
		}
	}

	class InternalNode extends Node {
		List<Node> children;

		InternalNode() {
			this.keys = new ArrayList<PrimitiveValue>();
			this.children = new ArrayList<Node>();
		}

		@Override
		String getValue(PrimitiveValue key) {
			return getChild(key).getValue(key);
		}

		@Override
		Node insertValue(PrimitiveValue key, ArrayList<PrimitiveValue> value) {
			Node child = getChild(key);
			Node returnedChild = child.insertValue(key, value);
			
			if (!returnedChild.equals(child)) {
				insertChild(returnedChild.getFirstLeafKey(), returnedChild);
			}
			
//			if (child.isOverflow()) {
//				Node sibling = child.split();
//				insertChild(sibling.getFirstLeafKey(), sibling);
//			}
			if (root.isOverflow()) {
				Node sibling = split();
				InternalNode newRoot = new InternalNode();
				newRoot.keys.add(sibling.getFirstLeafKey());
				newRoot.children.add(this);
				newRoot.children.add(sibling);
				return newRoot;
			}
			
			return this;
		}

		@Override
		PrimitiveValue getFirstLeafKey() {
			return children.get(0).getFirstLeafKey();
		}

		@Override
		Node getFirstLeafNode() {
			return children.get(0).getFirstLeafNode();
		}

		@Override
		List<String> getRange(PrimitiveValue key1, RangePolicy policy1, PrimitiveValue key2, RangePolicy policy2) {
			return getChild(key1).getRange(key1, policy1, key2, policy2);
		}

		void merge(Node sibling) {
			InternalNode node = (InternalNode) sibling;
			keys.add(node.getFirstLeafKey());
			keys.addAll(node.keys);
			children.addAll(node.children);

		}

		@Override
		Node split() {
			int from = keyNumber() / 2 + 1, to = keyNumber();
			InternalNode sibling = new InternalNode();
			sibling.keys.addAll(keys.subList(from, to));
			sibling.children.addAll(children.subList(from, to + 1));

			keys.subList(from - 1, to).clear();
			children.subList(from, to + 1).clear();

			return sibling;
		}

		@Override
		boolean isOverflow() {
			return children.size() > branchingFactor;
		}

		@Override
		boolean isUnderflow() {
			return children.size() < (branchingFactor + 1) / 2;
		}

		Node getChild(PrimitiveValue key) {
			int loc = Collections.binarySearch(keys, key, c);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			return children.get(childIndex);
		}

		void insertChild(PrimitiveValue key, Node child) {
			int loc = Collections.binarySearch(keys, key, c);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			if (loc >= 0) {
				children.set(childIndex, child);
			} else {
				keys.add(childIndex, key);
				children.add(childIndex + 1, child);
			}
		}

		Node getChildLeftSibling(PrimitiveValue key) {
			int loc = Collections.binarySearch(keys, key, c);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			if (childIndex > 0)
				return children.get(childIndex - 1);

			return null;
		}

		Node getChildRightSibling(PrimitiveValue key) {
			int loc = Collections.binarySearch(keys, key, c);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			if (childIndex < keyNumber())
				return children.get(childIndex + 1);

			return null;
		}
	}
}
