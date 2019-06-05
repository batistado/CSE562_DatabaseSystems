package dubstep;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import Indexes.PrimaryIndex;
import Indexes.Indexer;
import Indexes.LinearPrimaryIndex;
import Indexes.LinearSecondaryIndex;
import Indexes.Position;
import Indexes.TreeSearch;
import Iterators.AggregationIterator;
import Iterators.CrossProductIterator;
import Iterators.FromIterator;
import Iterators.GroupByIterator;
import Iterators.InMemoryGroupByIterator;
import Iterators.IndexIterator;
import Iterators.InsertIterator;
import Iterators.LimitIterator;
import Iterators.LinearIndexIterator;
import Iterators.ProjectIterator;
import Iterators.RAIterator;
import Iterators.SelectIterator;
import Iterators.SortIterator;
import Iterators.SubQueryIterator;
import Iterators.UnionIterator;
import Utils.Optimizer;
import Utils.utils;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

public class QueryEvaluator {
	public RAIterator evaluatePlainSelect(PlainSelect plainSelectQuery) {
		FromItem fromItem = plainSelectQuery.getFromItem();

		RAIterator innerIterator = null;
		if (fromItem == null) {
			// Implement expression evaluation
			return null;
		} else if (fromItem instanceof SubSelect) {
			innerIterator = evaluateSubQuery(plainSelectQuery);
		} else {
			innerIterator = evaluateFromTables(plainSelectQuery);
		}

		innerIterator = addProjection(innerIterator, plainSelectQuery);
		innerIterator = addSort(innerIterator, plainSelectQuery.getOrderByElements());
		innerIterator = addLimit(innerIterator, plainSelectQuery.getLimit());

		return innerIterator;
	}

	public RAIterator addProjection(RAIterator iterator, PlainSelect plainSelect) {
		List<Column> groupByColumns = plainSelect.getGroupByColumnReferences();

		if (groupByColumns != null && !groupByColumns.isEmpty()) {
			RAIterator aggIterator = null;
			if (!Main.isInMemory)
				aggIterator = new AggregationIterator(
						new GroupByIterator(new Optimizer().optimizeRA(iterator), groupByColumns),
						plainSelect.getSelectItems());
			else
				aggIterator = new InMemoryGroupByIterator(iterator,
						plainSelect.getSelectItems());

			return plainSelect.getHaving() == null ? aggIterator
					: new SelectIterator(aggIterator, plainSelect.getHaving());
		}

		boolean hasAggregation = false;
		for (SelectItem selectItem : plainSelect.getSelectItems()) {
			if (selectItem instanceof SelectExpressionItem
					&& ((SelectExpressionItem) selectItem).getExpression() instanceof Function) {
				hasAggregation = true;
				break;
			}
		}

		return hasAggregation ? new AggregationIterator(iterator, plainSelect.getSelectItems())
				: new ProjectIterator(iterator, plainSelect.getSelectItems());
	}

	public RAIterator addLimit(RAIterator iterator, Limit limit) {
		if (limit == null)
			return iterator;

		return new LimitIterator(iterator, limit);
	}

	public RAIterator addSort(RAIterator iterator, List<OrderByElement> orderByElements) {
		if (orderByElements == null || orderByElements.isEmpty())
			return iterator;

		return new SortIterator(iterator, orderByElements);
	}

	public RAIterator evaluateSubQuery(PlainSelect selectQuery) {
		Expression where = selectQuery.getWhere();
		SubSelect subQuery = (SubSelect) selectQuery.getFromItem();
		List<Join> joins = selectQuery.getJoins();

		if (joins != null && !joins.isEmpty()) {
			return evaluateSubQueryJoins(subQuery, joins, where);
		} else {
			// Write Union logic
			PlainSelect subSelect = (PlainSelect) subQuery.getSelectBody();
			return where == null ? new SubQueryIterator(evaluatePlainSelect(subSelect), subQuery.getAlias())
					: new SelectIterator(new SubQueryIterator(evaluatePlainSelect(subSelect), subQuery.getAlias()),
							where);
		}

	}

	public RAIterator evaluateSubQueryJoins(SubSelect leftSubSelect, List<Join> joins, Expression filter) {
		RAIterator iterator = null;

		if (joins.size() == 1) {
			SubSelect rightSubSelect = (SubSelect) joins.get(0).getRightItem();
			iterator = new CrossProductIterator(
					new SubQueryIterator(evaluatePlainSelect((PlainSelect) leftSubSelect.getSelectBody()),
							leftSubSelect.getAlias()),
					new SubQueryIterator(evaluatePlainSelect((PlainSelect) rightSubSelect.getSelectBody()),
							rightSubSelect.getAlias()));
		} else {
			Collections.reverse(joins);

			RAIterator rightIterator = null;
			for (Join join : joins) {
				SubSelect rightSubSelect = (SubSelect) join.getRightItem();

				if (rightIterator == null) {
					rightIterator = new SubQueryIterator(
							evaluatePlainSelect((PlainSelect) rightSubSelect.getSelectBody()),
							rightSubSelect.getAlias());
				} else {
					rightIterator = new CrossProductIterator(
							new SubQueryIterator(evaluatePlainSelect((PlainSelect) rightSubSelect.getSelectBody()),
									rightSubSelect.getAlias()),
							rightIterator);
				}
			}

			iterator = new CrossProductIterator(
					new SubQueryIterator(evaluatePlainSelect((PlainSelect) leftSubSelect.getSelectBody()),
							leftSubSelect.getAlias()),
					rightIterator);
		}

		return filter == null ? iterator : new SelectIterator(iterator, filter);
	}

	public RAIterator evaluateUnion(Union union) {
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

	public RAIterator evaluateFromTables(PlainSelect plainSelect) {
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
	
	public RAIterator addChanges(Table fromTable) {
		FromIterator fromIterator = new FromIterator(fromTable);
		RAIterator iterator = fromIterator;
		Expression selectCondition = null;
		
		if (Main.inserts.containsKey(fromTable.getName())) {
			List<RAIterator> iterators = new ArrayList<RAIterator>();
			iterators.add(fromIterator);
			iterators.add(new InsertIterator(fromTable));
			iterator = new UnionIterator(iterators);
		}
		
		if (Main.deletes.containsKey(fromTable.getName())) {
			List<Expression> deleteConditions = Main.deletes.get(fromTable.getName());
			
			if (deleteConditions.size() == 1) {
				selectCondition = new InverseExpression(deleteConditions.get(0));
			} else {
				
				BinaryExpression exp = new OrExpression();
				exp.setLeftExpression(deleteConditions.get(0));
				exp.setRightExpression(deleteConditions.get(1));
				
				for (int i = 2; i < deleteConditions.size(); i++) {
					BinaryExpression be = new OrExpression();
					be.setLeftExpression(exp);
					be.setRightExpression(deleteConditions.get(i));
					exp = be;
				}
				
				selectCondition = new InverseExpression(exp);
			}
		}
		
		if (selectCondition != null) {
			iterator = new SelectIterator(iterator, selectCondition);
		}
		
		return iterator;
	}

	public RAIterator evaluateFromTable(Table fromTable, Expression where) {
		RAIterator iterator = addChanges(fromTable);
		if (where == null)
			return iterator;
		
		return new SelectIterator(iterator, where);
	}
	
	

	public RAIterator evaluateJoins(Table fromTable, List<Join> joins, Expression filter) {
		RAIterator leftIterator = addChanges(fromTable);
		for (Join join : joins) {
			Table leftTable = (Table) join.getRightItem();
			leftIterator = new CrossProductIterator(leftIterator, addChanges(leftTable));
		}

		return filter == null ? leftIterator : new SelectIterator(leftIterator, filter);
	}

	public RAIterator evaluateQuery(Select selectQuery) {
		if (selectQuery.getSelectBody() instanceof PlainSelect) {
			return evaluatePlainSelect((PlainSelect) selectQuery.getSelectBody());
		} else {
			return evaluateUnion((Union) selectQuery.getSelectBody());
		}
	}
}
