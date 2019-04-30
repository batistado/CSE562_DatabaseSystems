package Aggregates;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;

public interface Aggregate {
	public void addValue(PrimitiveValue newValue);
	public PrimitiveValue getValue();
	public Integer getIndex();
	public Expression getExpression();
	public Integer getDataType();
	public void resetAndAddValue(PrimitiveValue newValue);
}
