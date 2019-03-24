package Aggregates;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.schema.PrimitiveType;

public class Average implements Aggregate {
	private PrimitiveValue accumulator;
	private Float count;
	private PrimitiveType type;
	private Integer index;
	private Expression expression;

	public Average(String type, Integer index, Expression expression) {
		if (type.equals("int")) {
			accumulator = new LongValue(0);
			this.type = PrimitiveType.LONG;
		} else {
			accumulator = new DoubleValue(0);
			this.type = PrimitiveType.DOUBLE;
		}

		this.count = (float) 0.0;
		this.index = index;
		this.expression = expression;
	}
	
	public void setType(PrimitiveValue val) {
		if (type == PrimitiveType.DOUBLE)
			return;
		
		if (val.getType() == PrimitiveType.DOUBLE) {
			type = PrimitiveType.DOUBLE;
		}
	}

	public void addValue(PrimitiveValue newValue) {
		setType(newValue);
		
		try {
			if (type == PrimitiveType.LONG) {
				accumulator = new LongValue(accumulator.toLong() + newValue.toLong());
			} else {
				accumulator = new DoubleValue(accumulator.getType() == PrimitiveType.LONG ? accumulator.toLong() : accumulator.toDouble() + newValue.toDouble());
			}
			count++;
		} catch (InvalidPrimitive e) {
			e.printStackTrace();
		}
	}
	
	public Expression getExpression() {
		return this.expression;
	}

	public PrimitiveValue getValue() {
		try {
			accumulator = new DoubleValue(accumulator.toDouble() / count);
			
			return accumulator;
		} catch (InvalidPrimitive e) {
			e.printStackTrace();
			return null;
		}
	}

	public Integer getIndex() {
		return index;
	}

	@Override
	public void resetAndAddValue(PrimitiveValue newValue) {
		// TODO Auto-generated method stub
		if (type == PrimitiveType.LONG) {
			accumulator = new LongValue(0);
		} else {
			accumulator = new DoubleValue(0);
		}

		this.count = (float) 0.0;
		
		this.addValue(newValue);
	}
}