package Utils;

import java.sql.SQLException;
import java.util.ArrayList;

import Models.TupleSchema;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.schema.Column;

public class EvalClass extends Eval {
	private TupleSchema tupleSchema;
	private ArrayList<PrimitiveValue> row;
	private Expression expression;

	@Override
	public PrimitiveValue eval(Column col) throws SQLException {
		// TODO Auto-generated method stub
		int colID = tupleSchema.getSchemaByName(utils.getColumnName(col)).getColumnIndex();
		return row.get(colID);
	}
	
	@Override
	public PrimitiveValue eval(Function function) throws SQLException {
		if (function.getName().equals("DATE")) {
			String dateString = utils.getDate(function);
			return new DateValue(dateString);
		}
		
		int colID = tupleSchema.getSchemaByName(utils.getFunctionName(function)).getColumnIndex();
		return row.get(colID);
	}

	public TupleSchema getTupleSchema() {
		return tupleSchema;
	}

	public void setTupleSchema(TupleSchema tupleSchema) {
		this.tupleSchema = tupleSchema;
	}

	public ArrayList<PrimitiveValue> getRow() {
		return row;
	}

	public void setRow(ArrayList<PrimitiveValue> row) {
		this.row = row;
	}

	public Expression getExpression() {
		return expression;
	}

	public void setExpression(Expression expression) {
		this.expression = expression;
	}
	
	public boolean filterRow() {
		try {
			return this.eval(expression).toBool();
		} catch (InvalidPrimitive e) {
			// TODO Auto-generated catch block
			System.out.println(expression.toString());
			System.out.println(row.toString());
			System.out.println(tupleSchema.toString());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(expression.toString());
			System.out.println(row.toString());
			System.out.println(tupleSchema.toString());
		}
		return false;
	}
	
	public PrimitiveValue projectColumnValue() {
		try {
			return this.eval(expression);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
