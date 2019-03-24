package Aggregates;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.schema.PrimitiveType;

public class Sum implements Aggregate {
	private PrimitiveValue accumulator;
	private PrimitiveType type;
	private Integer index;
	private Expression expression;

	public Sum(String type, Integer index, Expression expression) {
		if (type.equals("int")) {
			accumulator = new LongValue(0);
			this.type = PrimitiveType.LONG;
		} else {
			accumulator = new DoubleValue(0);
			this.type = PrimitiveType.DOUBLE;
		}
		
		this.index = index;
		this.expression = expression;
	}

	public void addValue(PrimitiveValue newValue) {
		try {
			if (type == PrimitiveType.LONG) {
				accumulator = new LongValue(accumulator.toLong() + newValue.toLong());
			} else {
				accumulator = new DoubleValue(accumulator.toDouble() + newValue.toDouble());
			}
		} catch (InvalidPrimitive e) {
			e.printStackTrace();
		}

	}
	
	public PrimitiveValue getValue() {
		return accumulator;
	}
	
	public Integer getIndex() {
		return index;
	}

	@Override
	public Expression getExpression() {
		// TODO Auto-generated method stub
		return this.expression;
	}
	
	@Override
	public void resetAndAddValue(PrimitiveValue newValue) {
		// TODO Auto-generated method stub
		if (type == PrimitiveType.LONG) {
			accumulator = new LongValue(0);
		} else {
			accumulator = new DoubleValue(0);
		}
		
		this.addValue(newValue);
	}
}
