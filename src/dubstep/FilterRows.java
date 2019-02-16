package dubstep;

import java.sql.SQLException;
import java.util.ArrayList;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

public class FilterRows {
	
	public static Boolean filterRow(ArrayList<PrimitiveValue> unfilteredRow, Expression expression, String tableName){
		Eval eval = new Eval() {
			@Override
			public PrimitiveValue eval(Column col) throws SQLException {
				// TODO Auto-generated method stub
				String fullColumnName = tableName + "." + col.getColumnName();
				int colID = Main.tableSchemas.get(tableName).getSchemaByName(fullColumnName).getColumnIndex();
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
	
	public static Boolean filterRowForJoin(ArrayList<PrimitiveValue> unfilteredRow, Expression expression, String tableName){
		Eval eval = new Eval() {
			@Override
			public PrimitiveValue eval(Column col) throws SQLException {
				// TODO Auto-generated method stub
				
				String fullColumnName = tableName + "." + col.getTable().getName() + "." + col.getColumnName();
				TupleSchema s = Main.tableSchemas.get(tableName);
				int colID = s.getSchemaByName(fullColumnName).getColumnIndex();
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
