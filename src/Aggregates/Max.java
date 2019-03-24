package Aggregates;

import java.sql.Date;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.schema.PrimitiveType;

public class Max implements Aggregate {
	private PrimitiveValue accumulator;
	private PrimitiveType type;
	private Integer index;
	private Expression expression;

	public Max(String type, Integer index, Expression expression) {
		if (type.equals("int")) {
			accumulator = new LongValue(Long.MIN_VALUE);
			this.type = PrimitiveType.LONG;
		} else if (type.equals("date")) {
			accumulator = new DateValue("Sun Dec 02 22:47:04 BDT 292269055");
			this.type = PrimitiveType.DATE;
		}
		else {
			accumulator = new DoubleValue(Double.MIN_VALUE);
			this.type = PrimitiveType.DOUBLE;
		}
		
		this.index = index;
		this.expression = expression;
	}

	public void addValue(PrimitiveValue newValue) {
		try {
			if (type == PrimitiveType.LONG) {
				accumulator = new LongValue(newValue.toLong() > accumulator.toLong() ? newValue.toLong() : accumulator.toLong());
			} else if (type == PrimitiveType.DATE) {
				accumulator = new DateValue(((Date) newValue).compareTo((Date) accumulator) > 0 ? ((Date) newValue).toString() : ((Date) accumulator).toString());
			}
			else {
				accumulator = new DoubleValue(newValue.toDouble() > accumulator.toDouble() ? newValue.toDouble() : accumulator.toDouble());
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
			accumulator = new LongValue(Long.MIN_VALUE);
		} else if (type == PrimitiveType.DATE) {
			accumulator = new DateValue("Sun Dec 02 22:47:04 BDT 292269055");
		} else {
			accumulator = new DoubleValue(Double.MIN_VALUE);
		}
		
		this.addValue(newValue);
	}
}

