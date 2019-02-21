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
				String columnTableName = col.getTable().getName();
				String fullColumnName;
				
//				if (columnTableName == null) {
					// Case for simple queries where the dot convention is not required
					fullColumnName = tableName + "." + col.getColumnName();
//				} else {
//					// Case for full schema dot convention and joins
//					fullColumnName = columnTableName.equals(tableName) ? tableName + "." + col.getColumnName() : tableName + "." + columnTableName + "." + col.getColumnName();
//				}
				
				int colID = Main.tableSchemas.get(tableName).getSchemaByName(fullColumnName).getColumnIndex();
				return unfilteredRow.get(colID);
			}
		};
		
		try {
			return eval.eval(expression).toBool();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
	}
	
	public static PrimitiveValue filterRowForProjection(ArrayList<PrimitiveValue> unfilteredRow, Expression expression, String tableName){
		Eval eval = new Eval() {
			@Override
			public PrimitiveValue eval(Column col) throws SQLException {
				// TODO Auto-generated method stub
				String columnTableName = col.getTable().getName();
				
				String fullColumnName;
				
				if (columnTableName == null) {
					// Case for simple queries where the dot convention is not required
					fullColumnName = tableName + "." + col.getColumnName();
				} else {
					// Case for full schema dot convention and joins
					fullColumnName = columnTableName.equals(tableName) ? tableName + "." + col.getColumnName() : tableName + "." + columnTableName + "." + col.getColumnName();
				}
				
				int colID = Main.tableSchemas.get(tableName).getSchemaByName(fullColumnName).getColumnIndex();
				return unfilteredRow.get(colID);
			}
		};
		
		try {
			return eval.eval(expression);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
	}
}
