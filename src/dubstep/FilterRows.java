package dubstep;

import java.sql.SQLException;
import java.util.ArrayList;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

public class FilterRows {
	
	public static Boolean filterRow(ArrayList<PrimitiveValue> unfilteredRow, Expression expression){
		Eval eval = new Eval() {
			@Override
			public PrimitiveValue eval(Column col) throws SQLException {
				// TODO Auto-generated method stub
				Main main = new Main();
				int colID = main.schema.get(col.getColumnName());
				return unfilteredRow.get(colID);
			}
			
		};
		
		try {
			PrimitiveValue v = eval.eval(expression);
			return v.toBool();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
	}

}
