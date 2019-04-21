package Indexes;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import Iterators.RAIterator;
import Utils.utils;
import dubstep.Main;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class PrimaryIndex implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static enum RangePolicy {
		EXCLUSIVE, INCLUSIVE
	}

	private int branchingFactor;

	private Node root;
    transient private Table table;
	transient private List<Column> indexOnElements;
	transient private List<OrderByElement> orderByElements;

	public PrimaryIndex(Table table, List<Column> indexOnElements) {
		this(50000, table, indexOnElements);
	}

	public PrimaryIndex(int branchingFactor, Table table, List<Column> indexOnElements) {
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

	public void insert(ArrayList<PrimitiveValue> value) {
		PrimitiveValue key = utils.projectColumnValue(value, indexOnElements.get(0),
				Main.tableSchemas.get(utils.getTableName(table)));

		root = root.insertValue(key, value);
	}

	private class LeafNode extends Node implements Serializable {
		/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
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
			int loc = Collections.binarySearch(keys, key, utils.c);
			int valueIndex = loc >= 0 ? loc : -loc - 1;

			if (loc < 0) {
				keys.add(valueIndex, key);
			}
			
				
			if (buffer.size() > branchingFactor - 1 && !utils.areEqual(key, lastKey)) {
				Node sibling = split();
				((LeafNode) sibling).insertValue(key, value);
				InternalNode newRoot = new InternalNode();
				newRoot.keys.add(sibling.getFirstLeafKey());
				newRoot.children.add(this);
				newRoot.children.add(sibling);
				return newRoot;
				
			} else {
				buffer.add(value);
				
				if (loc < 0) {
					lastKey = key;
				}
			}
			
			return this;
		}

//		public void sort(LinkedList<ArrayList<PrimitiveValue>> buffer, List<OrderByElement> orderByElements,
//				TupleSchema fromSchema) {
//			try {
//				Collections.sort(buffer, new Comparator<ArrayList<PrimitiveValue>>() {
//					@Override
//					public int compare(ArrayList<PrimitiveValue> o1, ArrayList<PrimitiveValue> o2) {
//						// TODO Auto-generated method stub
//						return utils.sortComparator(o1, o2, orderByElements, fromSchema);
//					}
//				});
//			} catch (IllegalArgumentException e) {
//				e.printStackTrace();
//			}
//		}

		private String writeToFile(List<ArrayList<PrimitiveValue>> rows, String fileName) {
			File temp = null;

			try {
				temp = File.createTempFile("Temp", ".csv", new File(RAIterator.TEMP_DIR));
				BufferedWriter bw = new BufferedWriter(new FileWriter(temp));

				for (ArrayList<PrimitiveValue> row: rows) {
					bw.write(utils.getOutputString(row));
					bw.write("\n");
				}
				
				rows.clear();
				
				bw.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return temp.getName();

		}

		@Override
		PrimitiveValue getFirstLeafKey() {
			return keys.get(0);
		}
		
		PrimitiveValue getLastLeafKey() {
			return this.lastKey;
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
			List<String> result = new ArrayList<String>();

			LeafNode node = this;
			while (node != null) {
				
				Integer cmp1 = null;
				Integer cmp2 = null;

				if (left != null) {
					cmp1 = utils.primitiveValueComparator(node.getLastLeafKey(), left);
				}

				if (right != null) {
					cmp2 = utils.primitiveValueComparator(node.getFirstLeafKey(), right);
				}

				if (cmp1 != null) {
					if ((leftPolicy == RangePolicy.EXCLUSIVE && cmp1 > 0)
							|| (leftPolicy == RangePolicy.INCLUSIVE && cmp1 >= 0)) {
						if (cmp2 == null || (rightPolicy == RangePolicy.EXCLUSIVE && cmp2 < 0)
								|| (rightPolicy == RangePolicy.INCLUSIVE && cmp2 <= 0)) {
							result.add(node.fileName);
						} else
							return result;
					}
				} else {
					if ((rightPolicy == RangePolicy.EXCLUSIVE && cmp2 < 0)
							|| (rightPolicy == RangePolicy.INCLUSIVE && cmp2 <= 0)) {
						result.add(node.fileName);
					} else
						return result;
				}
				
				node = node.next;
			}
			
			return result;
		}

		@Override
		Node split() {
			LeafNode sibling = new LeafNode();
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

	public abstract class Node implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
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

	class InternalNode extends Node implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
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
			if (key1 == null)
				return this.getFirstLeafNode().getRange(key1, policy1, key2, policy2);
			
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
			int loc = Collections.binarySearch(keys, key, utils.c);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			return children.get(childIndex);
		}

		void insertChild(PrimitiveValue key, Node child) {
			int loc = Collections.binarySearch(keys, key, utils.c);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			if (loc >= 0) {
				children.set(childIndex, child);
			} else {
				keys.add(childIndex, key);
				children.add(childIndex + 1, child);
			}
		}

		Node getChildLeftSibling(PrimitiveValue key) {
			int loc = Collections.binarySearch(keys, key, utils.c);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			if (childIndex > 0)
				return children.get(childIndex - 1);

			return null;
		}

		Node getChildRightSibling(PrimitiveValue key) {
			int loc = Collections.binarySearch(keys, key, utils.c);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			if (childIndex < keyNumber())
				return children.get(childIndex + 1);

			return null;
		}
	}
}
