package Indexes;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import Iterators.RAIterator;
import Models.TupleSchema;
import Utils.utils;
import dubstep.Main;
import net.sf.jsqlparser.expression.PrimitiveValue;
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

	private static final int DEFAULT_BRANCHING_FACTOR = 128;
	private int branchingFactor;

	private Node root;
	private Table table;
	private List<Column> indexOnElements;
	private List<OrderByElement> orderByElements;

	public BPlusTree(Table table, List<Column> indexOnElements) {
		this(DEFAULT_BRANCHING_FACTOR, table, indexOnElements);
	}

	public BPlusTree(int branchingFactor, Table table, List<Column> indexOnElements) {
		if (branchingFactor <= 2)
			throw new IllegalArgumentException("Illegal branching factor: " + branchingFactor);
		this.branchingFactor = branchingFactor;
		root = new LeafNode();
		this.table = table;
		this.indexOnElements = indexOnElements;

		setIndexOnElements();
	}
	
	public void closeIndex() {
		LeafNode head = (LeafNode) root.getFirstLeafNode();
		
		while (head != null) {
			head.writeBuffersToDisk();
			head = head.next;
		}
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
		root.insertValue(key, value);
	}

	private class LeafNode extends Node {
		List<String> values;
		List<ArrayList<ArrayList<PrimitiveValue>>> buffers;
		LeafNode next;

		LeafNode() {
			buffers = new ArrayList<ArrayList<ArrayList<PrimitiveValue>>>();
			keys = new ArrayList<PrimitiveValue>();
			values = new ArrayList<String>();
		}

		@Override
		String getValue(PrimitiveValue key) {
			int loc = Collections.binarySearch(keys, key, c);
			return loc >= 0 ? values.get(loc) : null;
		}

		@Override
		void insertValue(PrimitiveValue key, ArrayList<PrimitiveValue> value) {
			int loc = Collections.binarySearch(keys, key, c);
			int valueIndex = loc >= 0 ? loc : -loc - 1;
			
			// Key already exists
			if (loc >= 0) {
				ArrayList<ArrayList<PrimitiveValue>> buffer;

				if (values.get(valueIndex) == null) {
//					if (valueIndex > buffers.size() - 1) {
//						buffer = new ArrayList<ArrayList<PrimitiveValue>>();
//					} else {
						buffer = buffers.get(valueIndex);
//					}

					buffer.add(value);
					
					sort(buffer, orderByElements, Main.tableSchemas.get(utils.getTableName(table)));

					if (buffer.size() > Main.sortBufferSize) {
						values.set(valueIndex, writeToFile(buffer));
						buffer = null;
						System.gc();
					}
					
					
				} else {
					values.set(valueIndex, addToFile(value, values.get(valueIndex)));
				}
			} else {
				keys.add(valueIndex, key);
				values.add(valueIndex, null);
				ArrayList<ArrayList<PrimitiveValue>> buffer = new ArrayList<ArrayList<PrimitiveValue>>();
				buffer.add(value);
				buffers.add(valueIndex, buffer);
				
//				ArrayList<ArrayList<PrimitiveValue>> tmp = new ArrayList<ArrayList<PrimitiveValue>>();
//				tmp.add(value);
//				values.add(valueIndex, writeToFile(tmp));
			}
			
			
			if (root.isOverflow()) {
				Node sibling = split();
				InternalNode newRoot = new InternalNode();
				newRoot.keys.add(sibling.getFirstLeafKey());
				newRoot.children.add(this);
				newRoot.children.add(sibling);
				root = newRoot;
			}
		}
		
		void writeBuffersToDisk() {
			for (int i = 0; i < values.size(); i++) {
				if (values.get(i) == null) {
					values.set(i, writeToFile(buffers.get(i)));
					buffers.get(i).clear();
				}
			}
			buffers = null;
		}

		public void sort(ArrayList<ArrayList<PrimitiveValue>> buffer, List<OrderByElement> orderByElements,
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

		private String addToFile(ArrayList<PrimitiveValue> row, String fileName) {
			try {
				BufferedReader br = new BufferedReader(new FileReader(RAIterator.TEMP_DIR + fileName));
				ArrayList<ArrayList<PrimitiveValue>> rows = new ArrayList<ArrayList<PrimitiveValue>>();

				String line = null;
				while ((line = br.readLine()) != null) {
					rows.add(utils.splitLine(line, table));
				}

				rows.add(row);

				br.close();
				sort(rows, orderByElements, Main.tableSchemas.get(utils.getTableName(table)));

				return writeToFile(rows);

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}

		private String writeToFile(ArrayList<ArrayList<PrimitiveValue>> rows) {
			try {
				File temp = File.createTempFile("Temp", ".csv", new File(RAIterator.TEMP_DIR));
				BufferedWriter bw = new BufferedWriter(new FileWriter(temp));
				for (ArrayList<PrimitiveValue> i : rows) {
					bw.write(utils.getOutputString(i));
					bw.write("\n");
					i = null;
				}
				rows = null;
				bw.flush();
				bw.close();

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

//		@Override
//		List<String> getRange(BinaryExpression binaryExpression) {
//			List<String> result = new LinkedList<String>();
//			BinaryExpression primaryExpression = getPrimaryExpression(binaryExpression);
//			
//			LeafNode node = this;
//			while (node != null) {
//				Iterator<PrimitiveValue> kIt = node.keys.iterator();
//				Iterator<String> vIt = node.values.iterator();
//				while (kIt.hasNext()) {
//					PrimitiveValue key = kIt.next();
//					String value = vIt.next();
//					
//					
//					
//					int cmp1 = key.compareTo(key1);
//					int cmp2 = key.compareTo(key2);
//					if (((policy1 == RangePolicy.EXCLUSIVE && cmp1 > 0) || (policy1 == RangePolicy.INCLUSIVE && cmp1 >= 0))
//							&& ((policy2 == RangePolicy.EXCLUSIVE && cmp2 < 0) || (policy2 == RangePolicy.INCLUSIVE && cmp2 <= 0)))
//						result.add(value);
//					else if ((policy2 == RangePolicy.EXCLUSIVE && cmp2 >= 0)
//							|| (policy2 == RangePolicy.INCLUSIVE && cmp2 > 0))
//						return result;
//				}
//				node = node.next;
//			}
//			return result;
//		}

		@Override
		List<String> getRange(PrimitiveValue left, RangePolicy leftPolicy, PrimitiveValue right,
				RangePolicy rightPolicy) {
			List<String> result = new LinkedList<String>();

			LeafNode node = this;
			while (node != null) {
				Iterator<PrimitiveValue> kIt = node.keys.iterator();
				Iterator<String> vIt = node.values.iterator();
				while (kIt.hasNext()) {
					PrimitiveValue key = kIt.next();
					String value = vIt.next();

					Integer cmp1 = null;
					Integer cmp2 = null;

					if (left != null) {
						cmp1 = utils.primitiveValueComparator(key, left);
					}

					if (right != null) {
						cmp2 = utils.primitiveValueComparator(key, right);
					}

					if (cmp1 != null) {
						if ((leftPolicy == RangePolicy.EXCLUSIVE && cmp1 > 0)
								|| (leftPolicy == RangePolicy.INCLUSIVE && cmp1 >= 0)) {
							if (cmp2 == null || (rightPolicy == RangePolicy.EXCLUSIVE && cmp2 < 0)
									|| (rightPolicy == RangePolicy.INCLUSIVE && cmp2 <= 0)) {
								result.add(value);
							} else
								return result;
						}
					} else {
						if ((rightPolicy == RangePolicy.EXCLUSIVE && cmp2 < 0)
								|| (rightPolicy == RangePolicy.INCLUSIVE && cmp2 <= 0)) {
							result.add(value);
						} else
							return result;
					}
					return result;
				}
				node = node.next;
			}
			return result;
		}

		@Override
		Node split() {
			LeafNode sibling = new LeafNode();
			int from = (keyNumber() + 1) / 2, to = keyNumber();
			sibling.keys.addAll(keys.subList(from, to));
			sibling.values.addAll(values.subList(from, to));
			sibling.buffers.addAll(buffers.subList(from, to));

			keys.subList(from, to).clear();
			values.subList(from, to).clear();
			buffers.subList(from, to).clear();

			sibling.next = next;
			next = sibling;
			return sibling;
		}

		@Override
		boolean isOverflow() {
			return values.size() > branchingFactor - 1;
		}

		@Override
		boolean isUnderflow() {
			return values.size() < branchingFactor / 2;
		}
	}

	public abstract class Node {
		List<PrimitiveValue> keys;

		int keyNumber() {
			return keys.size();
		}

		abstract String getValue(PrimitiveValue key);
		
		abstract Node getFirstLeafNode();

		abstract void insertValue(PrimitiveValue key, ArrayList<PrimitiveValue> value);

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
		void insertValue(PrimitiveValue key, ArrayList<PrimitiveValue> value) {
			Node child = getChild(key);
			child.insertValue(key, value);
			if (child.isOverflow()) {
				Node sibling = child.split();
				insertChild(sibling.getFirstLeafKey(), sibling);
			}
			if (root.isOverflow()) {
				Node sibling = split();
				InternalNode newRoot = new InternalNode();
				newRoot.keys.add(sibling.getFirstLeafKey());
				newRoot.children.add(this);
				newRoot.children.add(sibling);
				root = newRoot;
			}
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
