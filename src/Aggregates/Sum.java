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
	private Integer stringType;

	public Sum(Integer type, Integer index, Expression expression) {
		if (type == 2) {
			accumulator = new LongValue(0);
			this.type = PrimitiveType.LONG;
		} else {
			accumulator = new DoubleValue(0);
			this.type = PrimitiveType.DOUBLE;
		}
		
		this.stringType = type;
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
				accumulator = new DoubleValue((accumulator.getType() == PrimitiveType.LONG ? accumulator.toLong() : accumulator.toDouble()) + newValue.toDouble());
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
	
	@Override
	public Integer getDataType() {
		// TODO Auto-generated method stub
		return stringType;
	}
}
