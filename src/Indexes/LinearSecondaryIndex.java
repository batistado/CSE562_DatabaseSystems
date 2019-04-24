package Indexes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import Indexes.PrimaryIndex.RangePolicy;
import Utils.utils;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class LinearSecondaryIndex {
	ArrayList<PrimitiveValue> keys;
	ArrayList<HashSet<Position>> positions;
	public Table table;
	public Column column;

	public LinearSecondaryIndex() {
		keys = new ArrayList<PrimitiveValue>();
		positions = new ArrayList<HashSet<Position>>();
	}

	public LinearSecondaryIndex(Table table, Column column) {
		keys = new ArrayList<PrimitiveValue>();
		positions = new ArrayList<HashSet<Position>>();
		this.table = table;
		this.column = column;
	}

	public void insert(PrimitiveValue key, Position position) {

		_insert(key, position);
	}

	public void _insert(PrimitiveValue key, Position position) {
		int loc = Collections.binarySearch(keys, key, utils.c);
		int valueIndex = loc >= 0 ? loc : -loc - 1;

		if (loc >= 0) {
			HashSet<Position> positionSet = positions.get(loc);

			if (positionSet.contains(position)) {
				System.out.println("Already in");
			} else {
				positionSet.add(position);
			}
		} else {
			HashSet<Position> positionSet = new HashSet<Position>();
			positionSet.add(position);
			keys.add(valueIndex, key);
			positions.add(valueIndex, positionSet);
		}
	}

	public List<Position> search(PrimitiveValue key) {
		int loc = Collections.binarySearch(keys, key, utils.c);
//		int valueIndex = loc >= 0 ? loc : -loc - 1;

		if (loc >= 0) {
			return new ArrayList<Position>(positions.get(loc));
		}

		return null;
	}
//	
//	public void addRow(PrimitiveValue key, Long startPos, Long endPos) {
//		keys.add(key);
//		
//		Position pos = new Position();
//		
//		pos.startPosition = startPos;
//		pos.endPosition = endPos;
//		
//		positions.add(pos);
//	}

	public List<Position> searchRange(PrimitiveValue left, RangePolicy leftPolicy, PrimitiveValue right,
			RangePolicy rightPolicy) {
		HashSet<Position> result = new HashSet<Position>();

		int startIndex = 0;
		int valueIndex = 0;

		if (left != null) {
			startIndex = Collections.binarySearch(keys, left, utils.c);
			valueIndex = startIndex >= 0 ? startIndex : -startIndex - 1;
		}

		if (startIndex < 0) {
			if (valueIndex >= keys.size())
				return null;
			startIndex = 0;
		} else {
			if (leftPolicy == RangePolicy.EXCLUSIVE) {
			if (startIndex + 1 >= positions.size()) {
				return null;
			}
			startIndex++;
		}
		}

		int endIndex = keys.size() - 1;
		if (right != null) {
			endIndex = Collections.binarySearch(keys, right, utils.c);
			valueIndex = endIndex >= 0 ? endIndex : -endIndex - 1;
		}
		
		if (endIndex < 0) {
			if (valueIndex < 0)
				return null;
			endIndex = keys.size() - 1;
		} else {
			if (rightPolicy == RangePolicy.EXCLUSIVE) {
				if (endIndex - 1 < 0) {
					return null;
				}
				
				endIndex--;
		}
		}
		
		int currIndex = startIndex;
		
		while (currIndex <= endIndex) {
			result.addAll(positions.get(currIndex));
			currIndex++;
		}
		
		return new ArrayList<Position>(result);
	}
}
