package Utils;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import Models.TupleSchema;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public class utils {
	public static boolean areEqual(PrimitiveValue pa, PrimitiveValue pb) {
		try {
			if (pa instanceof LongValue && pb instanceof LongValue) {
				if (pa.toLong() == pb.toLong()) {
					return true;
				}
			} else if (pa instanceof DoubleValue && pb instanceof DoubleValue) {
				if (Double.compare(pa.toDouble(), pb.toDouble()) == 0) {
					return true;
				}
			} else if (pa instanceof DateValue && pb instanceof DateValue) {
				DateValue dpa = (DateValue) pa;
				DateValue dpb = (DateValue) pb;

				if ((dpa.getYear() * 10000 + dpa.getMonth() * 100 + dpa.getDate()) == (dpb.getYear() * 10000
						+ dpb.getMonth() * 100 + dpb.getDate())) {
					return true;
				}
			} else {
				return pa.toString().equals(pb.toString());
			}
			
			return false;
		} catch (InvalidPrimitive i) {
			i.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	
	
	public static String getOutputString(List<PrimitiveValue> resultList) {
	    StringBuffer sb = new StringBuffer();
	    for (PrimitiveValue value : resultList){
	        sb.append(value.toString().replaceAll("^\'|\'$", "")).append("|");
	    }
	    sb.deleteCharAt(sb.lastIndexOf("|"));
	    return sb.toString();
	}
	
	public static String getRandomString() {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < 18) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;
    }
	
	public static String getExpressionColumnDatatype(BinaryExpression binaryExpression, TupleSchema fromSchema) {
		Expression leftExpression = binaryExpression.getLeftExpression();
		if (leftExpression instanceof Column) {
			Column column = (Column) leftExpression;
			return fromSchema.getSchemaByName(column.getWholeColumnName()).getDataType();
		} else if (leftExpression instanceof PrimitiveValue) {
			PrimitiveValue pv = (PrimitiveValue) leftExpression;
			PrimitiveType pt = pv.getType();
			switch(pt.name()) 
	        { 
	            case "LONG": 
	                return "int"; 
	            case "STRING": 
	            	return "varchar"; 
	            case "DOUBLE": 
	                return "decimal";
	            case "DATE":
	            	return "date";
	            default: 
	                return "varchar"; 
	        } 
		}
		
		return getExpressionColumnDatatype((BinaryExpression) leftExpression, fromSchema);
	}
	
	public static String getTableName(Table table) {
		return table.getAlias() == null ? table.getName() : table.getAlias();
	}
	
	public static String getColumnName(SelectExpressionItem selectExpressionItem, String colName) {
		return selectExpressionItem.getAlias() == null ? colName : selectExpressionItem.getAlias();
	}
	
	public static String getColumnName(Column column) {
		String columnTableName = column.getTable().getName();
		return columnTableName == null ? column.getColumnName() : columnTableName + "." + column.getColumnName();
	}
	
	public static PrimitiveValue projectColumnValue(ArrayList<PrimitiveValue> row, Expression expression, TupleSchema tupleSchema){
		Eval eval = new Eval() {
			@Override
			public PrimitiveValue eval(Column col) throws SQLException {
				int colID = tupleSchema.getSchemaByName(getColumnName(col)).getColumnIndex();
				return row.get(colID);
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
	
	public static String getDate(Function f) {
		return f.getParameters().getExpressions().get(0).toString();
	}
	
	public static String getFunctionName(Function f) {
		if (f.getParameters() == null) {
			return "*";
		}
		
		String functionName = "";
		for (Expression e : f.getParameters().getExpressions()) {
			functionName += getNameFromExpression(e);
		}
		
		return functionName;
	}
	
	public static String getNameFromExpression(Expression e) {
		if (e instanceof Column) {
			Column col = (Column) e;
			return col.getWholeColumnName();
		}
		
		String name = "";
		
		if (!(e instanceof BinaryExpression))
			return e.toString();
		
		BinaryExpression binaryExpression = (BinaryExpression) e;
		
		name += getNameFromExpression(binaryExpression.getLeftExpression());
		
		if (binaryExpression instanceof Addition) {
			name += "+";
		} else if (binaryExpression instanceof Subtraction) {
			name += "-";
		} else if (binaryExpression instanceof Multiplication) {
			name += "*";
		} else if (binaryExpression instanceof Division) {
			name += "/";
		} else {
			name += "%";
		}
			
		name += getNameFromExpression(binaryExpression.getRightExpression());
		
		return name;
	}
	
	public static Integer tsToSec8601(String timestamp){
	  if(timestamp == null) return null;
	  
	  try {
	    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	    Date dt = sdf.parse(timestamp);
	    long epoch = dt.getTime();
	    return (int)(epoch/1000);
	  } catch(ParseException e) {
	     return null;
	  }
	}
	
	public static Boolean filterRow(ArrayList<PrimitiveValue> unfilteredRow, Expression expression, TupleSchema tupleSchema){
		Eval eval = new Eval() {
			@Override
			public PrimitiveValue eval(Column col) throws SQLException {
				int colID = tupleSchema.getSchemaByName(getColumnName(col)).getColumnIndex();
				return unfilteredRow.get(colID);
			}
			
			@Override
			public PrimitiveValue eval(Function function) throws SQLException {
				if (function.getName().equals("DATE")) {
					String dateString = getDate(function);
					return new DateValue(dateString.substring(1, dateString.length() - 1));
				}
				
				int colID = tupleSchema.getSchemaByName(getFunctionName(function)).getColumnIndex();
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
