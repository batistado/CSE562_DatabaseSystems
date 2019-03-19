package dubstep;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import Iterators.CrossProductIterator;
import Iterators.FromIterator;
import Iterators.PlainSelectIterator;
import Iterators.ProjectIterator;
import Iterators.RAIterator;
import Iterators.SelectIterator;
import Iterators.SubSelectIterator;
import Iterators.UnionIterator;
import Models.TupleSchema;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

public class Main {
	public static Map<String, TupleSchema> tableSchemas = new HashMap<>();
	
	
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		CCJSqlParser parser;
		
		System.out.println("$> ");
		parser = new CCJSqlParser(System.in);
		Statement queryStatement;
		try {
			while ((queryStatement = parser.Statement()) != null) {
				if (queryStatement instanceof Select) {
					Select selectQuery = (Select) queryStatement;
					printer(evaluateQuery(selectQuery));
				}
				else if (queryStatement instanceof CreateTable) {
					createTable((CreateTable) queryStatement);
				}
				System.out.println("$> ");
				parser = new CCJSqlParser(System.in);
			}
		} catch (ParseException e) {
			System.out.println("Can't read query" + args[0]);
			e.printStackTrace();
		}
	}
		
	public static void createTable(CreateTable table) {
		if (!tableSchemas.containsKey(table.getTable().getName())) {
			TupleSchema ts = new TupleSchema();
			Integer i = 0;
			for (ColumnDefinition columnDefinition : table.getColumnDefinitions()) {
				ts.addTuple(table.getTable().getName() + "." + columnDefinition.getColumnName(), i, columnDefinition.getColDataType().getDataType());
				i++;
			}
			tableSchemas.put(table.getTable().getName(), ts);
		}
	}
	
	public static RAIterator evaluatePlainSelect(PlainSelect plainSelectQuery) {
		FromItem fromItem = plainSelectQuery.getFromItem();
		
		RAIterator innerIterator = null;
		if (fromItem == null) {
			// Implement expression evaluation
			return null;
		}
		else if (fromItem instanceof SubSelect) {
			innerIterator =  evaluateSubQuery(plainSelectQuery);
		} 
		else {
			innerIterator = evaluateFromTables(plainSelectQuery);
		}
		
		return new ProjectIterator(innerIterator, plainSelectQuery.getSelectItems());
	}
	
	public static RAIterator evaluateSubQuery(PlainSelect selectQuery) {
		SubSelect subQuery = (SubSelect) selectQuery.getFromItem();
		if (subQuery.getSelectBody() instanceof PlainSelect) {
			PlainSelect subSelect = (PlainSelect)subQuery.getSelectBody();
			return new SubSelectIterator(evaluatePlainSelect(subSelect), selectQuery.getSelectItems(), selectQuery.getWhere());
		} 
		else {
			// Write Union logic
			return evaluateUnion((Union) subQuery.getSelectBody());
		}
		
	}
	
	public static RAIterator evaluateUnion(Union union){
		if (!union.isAll()) {
			// TODO: Union with distinct
			return null;
		}
		
		List<RAIterator> iteratorsList = new ArrayList<>();
		for (PlainSelect plainSelect : union.getPlainSelects()) {
			iteratorsList.add(evaluatePlainSelect(plainSelect));
		}
		
		return new UnionIterator(iteratorsList);
	}
	
	public static RAIterator evaluateFromTables(PlainSelect plainSelect) {
		Expression where = plainSelect.getWhere();
		Table fromTable = (Table) plainSelect.getFromItem();
		List<Join> joins = plainSelect.getJoins();
		
		if (joins == null || joins.isEmpty()) {
			return evaluateFromTable(fromTable, where);
		} else {
			// joins implementation
			return evaluateJoins(fromTable, joins, where);
		}
	}
	
	public static RAIterator evaluateFromTable(Table fromTable, Expression where) {
		FromIterator fromIterator = new FromIterator(fromTable);
		
		return where == null ? fromIterator : new SelectIterator(fromIterator, where);
	}
	
	public static RAIterator evaluateJoins(Table fromTable, List<Join> joins, Expression filter) {
		RAIterator iterator = null;
		
		if (joins.size() == 1) {
			Table rightTable = (Table) joins.get(0).getRightItem();
			iterator = new CrossProductIterator(new FromIterator(fromTable), new FromIterator(rightTable));
		} else {
			Collections.reverse(joins);
			
			RAIterator rightIterator = null;
			for (Join join: joins) {
				Table rightTable = (Table) join.getRightItem();
				
				if (rightIterator == null) {
					rightIterator = new FromIterator(rightTable);
				} else {
					rightIterator = new CrossProductIterator(new FromIterator(rightTable), rightIterator);
				}
			}
			
			iterator = new CrossProductIterator(new FromIterator(fromTable), rightIterator);
		}
		
		return filter == null ? iterator : new SelectIterator(iterator, filter);
	}
	
	public static RAIterator evaluateQuery(Select selectQuery) {
		if (selectQuery.getSelectBody() instanceof PlainSelect) {
			return evaluatePlainSelect((PlainSelect)selectQuery.getSelectBody());
		}
		else {
			return evaluateUnion((Union) selectQuery.getSelectBody());
		}
	}
	
	public static void printer(RAIterator iterator) throws FileNotFoundException, UnsupportedEncodingException {
		while (iterator.hasNext()) {
			System.out.println(getOutputString(iterator.next()));
		}
		System.out.println();
	}
	
	public static String getOutputString(List<PrimitiveValue> resultList) {
	    StringBuffer sb = new StringBuffer();
	    for (PrimitiveValue value : resultList){
	        sb.append(value.toString().replaceAll("^\'|\'$", "")).append("|");
	    }
	    sb.deleteCharAt(sb.lastIndexOf("|"));
	    return sb.toString();
	}
}
