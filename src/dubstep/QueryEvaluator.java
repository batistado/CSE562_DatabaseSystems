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
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
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
			// RAIterator aggIterator = new AggregationIterator(new
			// GroupByIterator(iterator, groupByColumns), plainSelect.getSelectItems());

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
		// return new SortIterator(iterator, orderByElements);
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

	public RAIterator evaluateFromTable(Table fromTable, Expression where) {
		FromIterator fromIterator = new FromIterator(fromTable);

		if (where == null)
			return fromIterator;

//		String colName = utils.getOneSideColumnName(where);
//
//		if (Indexer.indexMapping.containsKey(colName)) {
//			List<Position> positions = new ArrayList<Position>();
//
//			LinearPrimaryIndex tree = Indexer.indexMapping.get(colName);
//
//			List<TreeSearch> treeSearchObjects = utils.getSearchObject(where);
//
//			if (treeSearchObjects == null) {
//				return utils.checkAndAddSecondaryIndex(colName, where, fromIterator, fromTable);
//			}
////				return new SelectIterator(fromIterator, where);
//
//			Position searchObject;
//			for (TreeSearch treeSearchObject : treeSearchObjects) {
//				if (treeSearchObject.operation.equals("EQUALS")) {
//					searchObject = tree.search(treeSearchObject.leftValue);
//
//					if (searchObject != null) {
//						positions.add(searchObject);
//					}
//
//				} else {
//					searchObject = tree.searchRange(treeSearchObject.leftValue, treeSearchObject.leftPolicy,
//							treeSearchObject.rightValue, treeSearchObject.rightPolicy);
//
//					if (searchObject != null) {
//						positions.add(searchObject);
//					}
//
//				}
//			}
//
//			if (!positions.isEmpty())
//				return new LinearIndexIterator(fromTable, where, positions);
//		}
		
		return new SelectIterator(fromIterator, where);

		//return utils.checkAndAddSecondaryIndex(colName, where, fromIterator, fromTable);
	}
	
	

	public RAIterator evaluateJoins(Table fromTable, List<Join> joins, Expression filter) {
//		Join firstJoin = new Join();
//		firstJoin.setRightItem(fromTable);
//		firstJoin.setSimple(true);
//
//		joins.add(firstJoin);
//
//		Collections.sort(joins, new Comparator<Join>() {
//			@Override
//			public int compare(Join o1, Join o2) {
//				// TODO Auto-generated method stub
//				String table1 = ((Table) o1.getRightItem()).getName();
//				String table2 = ((Table) o2.getRightItem()).getName();
//
//				Integer left = Indexer.tableSizeMapping.get(table1);
//				Integer right = Indexer.tableSizeMapping.get(table2);
//
//				return left.compareTo(right);
//			}
//		});

		RAIterator leftIterator = new FromIterator(fromTable);
		for (Join join : joins) {
			Table leftTable = (Table) join.getRightItem();
			leftIterator = new CrossProductIterator(leftIterator, new FromIterator(leftTable));
		}

		return filter == null ? leftIterator : new SelectIterator(leftIterator, filter);

//		Join firstJoin = new Join();
//		firstJoin.setRightItem(fromTable);
//		firstJoin.setSimple(true);
//
//		joins.add(firstJoin);
//
//		Collections.sort(joins, new Comparator<Join>() {
//			@Override
//			public int compare(Join o1, Join o2) {
//				// TODO Auto-generated method stub
//				String table1 = ((Table) o1.getRightItem()).getName();
//				String table2 = ((Table) o2.getRightItem()).getName();
//
//				Integer left = Indexer.tableSizeMapping.get(table1);
//				Integer right = Indexer.tableSizeMapping.get(table2);
//
//				return left.compareTo(right);
//			}
//		});
//
//		RAIterator leftIterator = null;
//		for (Join join : joins) {
//			Table leftTable = (Table) join.getRightItem();
//
//			if (leftIterator == null) {
//				leftIterator = new FromIterator(leftTable);
//			} else {
//				leftIterator = new CrossProductIterator(leftIterator, new FromIterator(leftTable));
//			}
//		}
//
//		return filter == null ? leftIterator : new SelectIterator(leftIterator, filter);
	}

	public RAIterator evaluateQuery(Select selectQuery) {
		if (selectQuery.getSelectBody() instanceof PlainSelect) {
			return evaluatePlainSelect((PlainSelect) selectQuery.getSelectBody());
		} else {
			return evaluateUnion((Union) selectQuery.getSelectBody());
		}
	}
}
