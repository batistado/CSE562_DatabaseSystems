package dubstep;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import Iterators.PlainSelectIterator;
import Iterators.RAIterator;
import Iterators.SubSelectIterator;
import Iterators.UnionIterator;
import Models.TupleSchema;
import net.sf.jsqlparser.expression.Expression;
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
	
	
	public static void main(String[] args) {
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
		
		if (fromItem == null) {
			// Implement expression evaluation
			return null;
		}
		else if (fromItem instanceof SubSelect) {
			return evaluateSubQuery(plainSelectQuery);
		} 
		else {
			return evaluateFromTables(plainSelectQuery);
		}
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
		List<SelectItem> selectItems = plainSelect.getSelectItems();
		
		
		if (joins == null || joins.isEmpty()) {
			return new PlainSelectIterator(fromTable, where, selectItems);
		}
	
		return evaluateJoins(fromTable, joins, where, selectItems);
	}
	
	public static RAIterator evaluateJoins(Table fromTable, List<Join> joins, Expression filter, List<SelectItem> selectItems) {
		RAIterator iterator = new PlainSelectIterator(fromTable, filter, null);
		for (Join join: joins) {
			Table rightTable = (Table) join.getRightItem();
			iterator = new PlainSelectIterator(iterator, rightTable, filter, join.getOnExpression(), null);
		}
		
		iterator.addSelectItems(selectItems);
		
		return iterator;
	}
	
	public static RAIterator evaluateQuery(Select selectQuery) {
		if (selectQuery.getSelectBody() instanceof PlainSelect) {
			return evaluatePlainSelect((PlainSelect)selectQuery.getSelectBody());
		}
		else {
			return evaluateUnion((Union) selectQuery.getSelectBody());
		}
	}
	
	public static void printer(RAIterator iterator) {
		while (iterator.hasNext()) {
			System.out.println(iterator.next().toString().replace(", ", "|").replace("\'", "").replaceAll("[\\[.\\]]", ""));
		}
		System.out.println();
	}
}
