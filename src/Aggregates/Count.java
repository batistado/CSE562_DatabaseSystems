package Aggregates;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;

public class Count implements Aggregate {
	private Integer count;
	private Integer index;
	private Expression expression;

	public Count(Integer index, Expression expression) {
		this.count = 0;
		this.index = index;
		this.expression = expression;
	}

	public void addValue(PrimitiveValue newValue) {
		this.count++;
	}
	
	public PrimitiveValue getValue() {
		return new LongValue(this.count);
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
		this.count = 0;
		
		this.addValue(newValue);
	}

	@Override
	public String getDataType() {
		// TODO Auto-generated method stub
		return "int";
	}
}

