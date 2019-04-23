package Indexes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;

import Indexes.PrimaryIndex.RangePolicy;
import Utils.utils;
import dubstep.Main;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class LinearPrimaryIndex implements Serializable {
	ArrayList<PrimitiveValue> keys;
	ArrayList<Position> positions;

	public LinearPrimaryIndex() {
		keys = new ArrayList<PrimitiveValue>();
		positions = new ArrayList<Position>();
	}

	public void insert(ArrayList<PrimitiveValue> row, Column column, Table table, long position) {
		PrimitiveValue key = utils.projectColumnValue(row, column, Main.tableSchemas.get(utils.getTableName(table)));

		_insert(key, position);
	}

	public void _insert(PrimitiveValue key, Long position) {
		int loc = Collections.binarySearch(keys, key, utils.c);
		// int valueIndex = loc >= 0 ? loc : -loc - 1;

		if (loc >= 0) {
			positions.get(loc).endPosition = position;
		} else {
			Position positionObj = new Position();
			positionObj.startPosition = position;
			positionObj.endPosition = position;

			keys.add(key);
			positions.add(positionObj);
		}
	}

	public Position search(PrimitiveValue key) {
		int loc = Collections.binarySearch(keys, key, utils.c);
//		int valueIndex = loc >= 0 ? loc : -loc - 1;

		if (loc >= 0) {
			return positions.get(loc);
		}

		return null;
	}

	public Position searchRange(PrimitiveValue left, RangePolicy leftPolicy, PrimitiveValue right,
			RangePolicy rightPolicy) {
		Position result = new Position();

		int startIndex = 0;
		int valueIndex = 0;

		if (left != null) {
			startIndex = Collections.binarySearch(keys, left, utils.c);
			valueIndex = startIndex >= 0 ? startIndex : -startIndex - 1;
		}

		if (startIndex < 0) {
			if (valueIndex >= keys.size())
				return null;
			result.startPosition = positions.get(0).startPosition;
		} else {
			if (leftPolicy == RangePolicy.INCLUSIVE) {
				result.startPosition = positions.get(startIndex).startPosition;
			} else {
				if (startIndex + 1 >= positions.size()) {
					return null;
				}
				result.startPosition = positions.get(startIndex + 1).startPosition;
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
			result.endPosition = positions.get(keys.size() - 1).endPosition;
		} else {
			if (rightPolicy == RangePolicy.INCLUSIVE) {
				result.endPosition = positions.get(endIndex).endPosition;
			} else {
				if (endIndex - 1 < 0) {
					return null;
				}
				
				result.endPosition = positions.get(endIndex - 1).endPosition;
			}
		}
		
		return result;
	}
}
