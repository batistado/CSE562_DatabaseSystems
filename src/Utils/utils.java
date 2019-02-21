package Utils;

import java.sql.SQLException;
import java.util.ArrayList;

import dubstep.Main;
import dubstep.TupleSchema;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public class utils {
	public static String getTableName(Table table) {
		return table.getAlias() == null ? table.getName() : table.getAlias();
	}
	
	public static String getColumnName(SelectExpressionItem selectExpressionItem, String colName) {
		return selectExpressionItem.getAlias() == null ? colName : selectExpressionItem.getAlias();
	}
	
	public static PrimitiveValue filterRowForProjection(ArrayList<PrimitiveValue> unfilteredRow, Expression expression, TupleSchema tupleSchema){
		Eval eval = new Eval() {
			@Override
			public PrimitiveValue eval(Column col) throws SQLException {
				String columnTableName = col.getTable().getName();
				String fullColumnName = columnTableName == null ? col.getColumnName() : columnTableName + "." + col.getColumnName();
				int colID = tupleSchema.getSchemaByName(fullColumnName).getColumnIndex();
				return unfilteredRow.get(colID);
//				// TODO Auto-generated method stub
//				String columnTableName = col.getTable().getName();
//				
//				String fullColumnName;
//				
//				if (columnTableName == null) {
//					// Case for simple queries where the dot convention is not required
//					fullColumnName = tableName + "." + col.getColumnName();
//				} else {
//					// Case for full schema dot convention and joins
//					fullColumnName = columnTableName.equals(tableName) ? tableName + "." + col.getColumnName() : tableName + "." + columnTableName + "." + col.getColumnName();
//				}
//				
//				int colID = Main.tableSchemas.get(tableName).getSchemaByName(fullColumnName).getColumnIndex();
//				return unfilteredRow.get(colID);
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
	
	public static Boolean filterRow(ArrayList<PrimitiveValue> unfilteredRow, Expression expression, TupleSchema tupleSchema){
		Eval eval = new Eval() {
			@Override
			public PrimitiveValue eval(Column col) throws SQLException {
				String columnTableName = col.getTable().getName();
				String fullColumnName = columnTableName == null ? col.getColumnName() : columnTableName + "." + col.getColumnName();
				int colID = tupleSchema.getSchemaByName(fullColumnName).getColumnIndex();
				return unfilteredRow.get(colID);
			}
		};
		
		try {
			return eval.eval(expression).toBool();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		
	}
}
