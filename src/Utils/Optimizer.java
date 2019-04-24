package Utils;

import java.util.*;

import Indexes.PrimaryIndex;
import Indexes.Indexer;
import Indexes.LinearPrimaryIndex;
import Indexes.LinearSecondaryIndex;
import Indexes.Position;
import Indexes.TreeSearch;
import Iterators.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class Optimizer {
	public RAIterator optimizeRA(RAIterator root) {
		if (root instanceof SelectIterator) {
			SelectIterator selectIterator = (SelectIterator) root;

			if (selectIterator.getRightIterator() instanceof CrossProductIterator) {
				CrossProductIterator crossProductIterator = (CrossProductIterator) selectIterator.getRightIterator();

				return optimizeSelectionOverCross(crossProductIterator, selectIterator);
			}

			return new SelectIterator(optimizeRA(selectIterator.getRightIterator()), selectIterator.getExpression());
		}

		else if (root instanceof ProjectIterator) {
			ProjectIterator projectIterator = (ProjectIterator) root;
			return new ProjectIterator(optimizeRA(projectIterator.getRightIterator()),
					projectIterator.getSelectItems());
		}

		else if (root instanceof SubQueryIterator) {
			SubQueryIterator subQueryIterator = (SubQueryIterator) root;

			return new SubQueryIterator(optimizeRA(subQueryIterator.getRightIterator()), subQueryIterator.getAlias());
		}

		else if (root instanceof UnionIterator) {
			UnionIterator unionIterator = (UnionIterator) root;

			List<RAIterator> resultIterators = new ArrayList<RAIterator>();
			for (RAIterator iterator : unionIterator.getIterators()) {
				resultIterators.add(optimizeRA(iterator));
			}

			return new UnionIterator(resultIterators);
		}

		else if (root instanceof OnePassHashJoinIterator) {
			OnePassHashJoinIterator iterator = (OnePassHashJoinIterator) root;

			return new OnePassHashJoinIterator(optimizeRA(iterator.getLeftIterator()),
					optimizeRA(iterator.getRightIterator()), iterator.getExpression());
		}

		else if (root instanceof LimitIterator) {
			LimitIterator iterator = (LimitIterator) root;

			return new LimitIterator(optimizeRA(iterator.getRightIterator()), iterator.getLimit());
		}

		else if (root instanceof AggregationIterator) {
			AggregationIterator iterator = (AggregationIterator) root;

			return new AggregationIterator(optimizeRA(iterator.getRightIterator()), iterator.getSelectItems());
		}
		
//		else if (root instanceof InMemoryGroupByIterator) {
//			InMemoryGroupByIterator iterator = (InMemoryGroupByIterator) root;
//
//			return new InMemoryGroupByIterator(optimizeRA(iterator.getRightIterator()), iterator.getSelectItems());
//		}

		else {
			return root;
		}

	}

	public Expression mergeAndClauses(List<Expression> expressionList) {
		if (expressionList.isEmpty()) {
			return null;
		}

		if (expressionList.size() == 1) {
			return expressionList.remove(0);
		}

		Expression leftExpression = expressionList.get(0);

		for (int i = 1; i < expressionList.size(); i++) {
			leftExpression = new AndExpression(leftExpression, expressionList.get(i));

		}

		return leftExpression;
	}

	public Expression getEquiJoinCondition(List<Expression> expressions, RAIterator leftIterator,
			RAIterator rightIterator) {
		if (expressions.isEmpty()) {
			return null;
		}

		for (Expression e : expressions) {
			if (e instanceof EqualsTo) {
				EqualsTo equalsToExpression = (EqualsTo) e;

				if (equalsToExpression.getLeftExpression() instanceof Column
						&& equalsToExpression.getRightExpression() instanceof Column
						&& ((leftIterator.getIteratorSchema()
								.containsKey(utils.getColumnName((Column) equalsToExpression.getLeftExpression()))
								&& rightIterator.getIteratorSchema().containsKey(
										utils.getColumnName((Column) equalsToExpression.getRightExpression())))
								|| (leftIterator.getIteratorSchema().containsKey(
										utils.getColumnName((Column) equalsToExpression.getRightExpression()))
										&& rightIterator.getIteratorSchema().containsKey(utils
												.getColumnName((Column) equalsToExpression.getLeftExpression()))))) {

					expressions.remove(e);

					if ((leftIterator.getIteratorSchema()
							.containsKey(utils.getColumnName((Column) equalsToExpression.getRightExpression()))
							&& rightIterator.getIteratorSchema().containsKey(
									utils.getColumnName((Column) equalsToExpression.getLeftExpression())))) {
						return new EqualsTo(equalsToExpression.getRightExpression(),
								equalsToExpression.getLeftExpression());
					}

					return e;
				}
			}
		}

		return null;

	}

	public Expression getJoinCondition(List<Expression> expressions, RAIterator leftIterator,
			RAIterator rightIterator) {
		if (expressions.isEmpty()) {
			return null;
		}

		for (Expression e : expressions) {
			BinaryExpression binaryExpression = (BinaryExpression) e;

			if (binaryExpression.getLeftExpression() instanceof Column
					&& binaryExpression.getRightExpression() instanceof Column
					&& ((leftIterator.getIteratorSchema()
							.containsKey(utils.getColumnName((Column) binaryExpression.getLeftExpression()))
							&& rightIterator.getIteratorSchema()
									.containsKey(utils.getColumnName((Column) binaryExpression.getRightExpression())))
							|| (leftIterator.getIteratorSchema()
									.containsKey(utils.getColumnName((Column) binaryExpression.getRightExpression()))
									&& rightIterator.getIteratorSchema().containsKey(
											utils.getColumnName((Column) binaryExpression.getLeftExpression()))))) {

				expressions.remove(e);

				if ((leftIterator.getIteratorSchema()
						.containsKey(utils.getColumnName((Column) binaryExpression.getRightExpression()))
						&& rightIterator.getIteratorSchema()
								.containsKey(utils.getColumnName((Column) binaryExpression.getLeftExpression())))) {
					Expression tmp = binaryExpression.getLeftExpression();
					binaryExpression.setLeftExpression(binaryExpression.getRightExpression());
					binaryExpression.setRightExpression(tmp);
				}

				return binaryExpression;
			}
		}

		return null;

	}

	public RAIterator optimizeSelectionOverCross(CrossProductIterator crossProductIterator,
			SelectIterator selectIterator) {
		List<Expression> expressionList = utils.splitAndClauses(selectIterator.getExpression());

		RAIterator iterator = optimizeSelectionOverCrossHelper(crossProductIterator, expressionList, 0);
		Expression expression = mergeAndClauses(expressionList);

		return expression == null ? iterator : new SelectIterator(iterator, expression);
	}
	
	public RAIterator checkAndAddSecondaryIndex(String colName, Expression where, RAIterator leftIterator, Table fromTable, List<Expression> expressionList) {
		if (Indexer.secondaryIndexMapping.containsKey(colName)) {
			List<Position> positions = new ArrayList<Position>();

			LinearSecondaryIndex tree = Indexer.secondaryIndexMapping.get(colName);

			List<TreeSearch> treeSearchObjects = utils.getSearchObject(where);

			if (treeSearchObjects == null) {
				expressionList.add(where);
				return leftIterator;
			}

			List<Position> searchObject;
			for (TreeSearch treeSearchObject : treeSearchObjects) {
				if (treeSearchObject.operation.equals("EQUALS")) {
					searchObject = tree.search(treeSearchObject.leftValue);

					if (searchObject != null) {
						positions.addAll(searchObject);
					}

				} else {
					searchObject = tree.searchRange(treeSearchObject.leftValue, treeSearchObject.leftPolicy,
							treeSearchObject.rightValue, treeSearchObject.rightPolicy);

					if (searchObject != null) {
						positions.addAll(searchObject);
					}

				}
			}

			if (!positions.isEmpty()) {
				return new LinearIndexIterator(fromTable, where, positions);
			} else {
				expressionList.add(where);
				return leftIterator;
			}
		}
		
		expressionList.add(where);
		return leftIterator;
	}

	public RAIterator optimizeSelectionOverCrossHelper(CrossProductIterator crossProductIterator,
			List<Expression> expressionList, Integer level) {
		if (expressionList.isEmpty()) {
			return crossProductIterator;
		}

		RAIterator leftIterator = crossProductIterator.getLeftIterator();
		Expression selectExpression = getIteratorSpecificCondition(expressionList, leftIterator);

		if (selectExpression != null) {
			String colName = utils.getOneSideColumnName(selectExpression);
			// Check if it can use an index
			if (leftIterator instanceof FromIterator) {

				if (Indexer.indexMapping.containsKey(colName)) {
					List<Position> positions = new ArrayList<Position>();
					
					LinearPrimaryIndex tree = Indexer.indexMapping.get(colName);
					
					List<TreeSearch> treeSearchObjects = utils.getSearchObject(selectExpression);
					
					if (treeSearchObjects == null) {
						leftIterator = checkAndAddSecondaryIndex(colName, selectExpression, leftIterator, ((FromIterator)leftIterator).getTable(), expressionList);
					}
					
					Position searchObject;
					for (TreeSearch treeSearchObject: treeSearchObjects) {
						if (treeSearchObject.operation.equals("EQUALS")) {
							searchObject = tree.search(treeSearchObject.leftValue);
							
							if (searchObject != null) {
								positions.add(searchObject);
							}
							
						} else {
							searchObject = tree.searchRange(treeSearchObject.leftValue, treeSearchObject.leftPolicy, treeSearchObject.rightValue, treeSearchObject.rightPolicy);
							
							if (searchObject != null) {
								positions.add(searchObject);
							}
							
						}
					}
					
					if (!positions.isEmpty()) {
						leftIterator = new LinearIndexIterator(((FromIterator)leftIterator).getTable(), selectExpression, positions);
					} else {
						leftIterator = checkAndAddSecondaryIndex(colName, selectExpression, leftIterator, ((FromIterator)leftIterator).getTable(), expressionList);
					}
					
				} else {
					leftIterator = checkAndAddSecondaryIndex(colName, selectExpression, leftIterator, ((FromIterator)leftIterator).getTable(), expressionList);
				}
			} 
			else {
				leftIterator = checkAndAddSecondaryIndex(colName, selectExpression, leftIterator, ((FromIterator)leftIterator).getTable(), expressionList);
			}
		}

		if (crossProductIterator.getRightIterator() instanceof CrossProductIterator) {
			RAIterator rightIterator = optimizeSelectionOverCrossHelper(
					(CrossProductIterator) crossProductIterator.getRightIterator(), expressionList, level + 1);

			Expression anyJoinCondition = getJoinCondition(expressionList, leftIterator, rightIterator);

			if (anyJoinCondition != null) {
				Column col = (Column) ((BinaryExpression) anyJoinCondition).getLeftExpression();
				if (leftIterator instanceof FromIterator && Indexer.indexMapping.containsKey(col.getWholeColumnName())) {
					return new LeftLinearIndexNestedLoopJoinIterator((FromIterator) leftIterator, rightIterator,
							Indexer.indexMapping.get(col.getWholeColumnName()), anyJoinCondition);
				}
				else
					if (anyJoinCondition instanceof EqualsTo) {
					selectExpression = getIteratorSpecificCondition(expressionList, leftIterator);
					
					if (selectExpression != null) {
						leftIterator = new SelectIterator(leftIterator, selectExpression);
					}
					
					return new InMemorySMJIterator(leftIterator, rightIterator, anyJoinCondition);
				} 
				else {
					expressionList.add(anyJoinCondition);
				}
			}

			return new CrossProductIterator(leftIterator, rightIterator);
		}

		RAIterator rightIterator = crossProductIterator.getRightIterator();
		selectExpression = getIteratorSpecificCondition(expressionList, rightIterator);

		if (selectExpression != null) {
			// Check if it can use an index
			String colName = utils.getOneSideColumnName(selectExpression);
			if (rightIterator instanceof FromIterator) {

				if (Indexer.indexMapping.containsKey(colName)) {
					List<Position> positions = new ArrayList<Position>();
					
					LinearPrimaryIndex tree = Indexer.indexMapping.get(colName);
					
					List<TreeSearch> treeSearchObjects = utils.getSearchObject(selectExpression);
					
					if (treeSearchObjects == null) {
						rightIterator = checkAndAddSecondaryIndex(colName, selectExpression, rightIterator, ((FromIterator)rightIterator).getTable(), expressionList);
					}
					
					Position searchObject;
					for (TreeSearch treeSearchObject: treeSearchObjects) {
						if (treeSearchObject.operation.equals("EQUALS")) {
							searchObject = tree.search(treeSearchObject.leftValue);
							
							if (searchObject != null) {
								positions.add(searchObject);
							}
							
						} else {
							searchObject = tree.searchRange(treeSearchObject.leftValue, treeSearchObject.leftPolicy, treeSearchObject.rightValue, treeSearchObject.rightPolicy);
							
							if (searchObject != null) {
								positions.add(searchObject);
							}
							
						}
					}
					
					if (!positions.isEmpty()) {
						rightIterator = new LinearIndexIterator(((FromIterator)rightIterator).getTable(), selectExpression, positions);
					} else {
						rightIterator = checkAndAddSecondaryIndex(colName, selectExpression, rightIterator, ((FromIterator)rightIterator).getTable(), expressionList);
					}
					
				} else {
					rightIterator = checkAndAddSecondaryIndex(colName, selectExpression, rightIterator, ((FromIterator)rightIterator).getTable(), expressionList);
				}
			} else {
				rightIterator = checkAndAddSecondaryIndex(colName, selectExpression, rightIterator, ((FromIterator)rightIterator).getTable(), expressionList);
			}
		}
		
		Expression anyJoinCondition = getJoinCondition(expressionList, leftIterator, rightIterator);

		if (anyJoinCondition != null) {
			Column leftColumn = (Column) ((BinaryExpression) anyJoinCondition).getLeftExpression();
			Column rightColumn = (Column) ((BinaryExpression) anyJoinCondition).getRightExpression();
			
			if (rightIterator instanceof FromIterator && Indexer.indexMapping.containsKey(rightColumn.getWholeColumnName()) && leftIterator instanceof FromIterator && Indexer.indexMapping.containsKey(leftColumn.getWholeColumnName())) {
				String leftTableName = ((FromIterator) leftIterator).getTable().getName();
				String rightTableName = ((FromIterator) rightIterator).getTable().getName();
				
				Integer leftCount = Indexer.tableSizeMapping.get(leftTableName);
				Integer rightCount = Indexer.tableSizeMapping.get(rightTableName);
				
				if (leftCount > rightCount) {
					selectExpression = getIteratorSpecificCondition(expressionList, rightIterator);
					
					if (selectExpression != null) {
						rightIterator = new SelectIterator(rightIterator, selectExpression);
					}
					
					return new LeftLinearIndexNestedLoopJoinIterator((FromIterator) leftIterator, rightIterator,
							Indexer.indexMapping.get(leftColumn.getWholeColumnName()), anyJoinCondition);
				} else {
					Expression swappedExpression = utils.swapLeftRightExpression(anyJoinCondition);
					
					selectExpression = getIteratorSpecificCondition(expressionList, leftIterator);
					
					if (selectExpression != null) {
						leftIterator = new SelectIterator(leftIterator, selectExpression);
					}
					
					return new RightLinearIndexNestedLoopJoinIterator((FromIterator) rightIterator, leftIterator,
							Indexer.indexMapping.get(rightColumn.getWholeColumnName()), swappedExpression);
				}
				
			}
			else if (rightIterator instanceof FromIterator && Indexer.indexMapping.containsKey(rightColumn.getWholeColumnName())) {
				Expression swappedExpression = utils.swapLeftRightExpression(anyJoinCondition);
				
				selectExpression = getIteratorSpecificCondition(expressionList, leftIterator);
				
				if (selectExpression != null) {
					leftIterator = new SelectIterator(leftIterator, selectExpression);
				}
				
				return new RightLinearIndexNestedLoopJoinIterator((FromIterator) rightIterator, leftIterator,
						Indexer.indexMapping.get(rightColumn.getWholeColumnName()), swappedExpression);
			}
			else if (leftIterator instanceof FromIterator && Indexer.indexMapping.containsKey(leftColumn.getWholeColumnName())) {
				selectExpression = getIteratorSpecificCondition(expressionList, rightIterator);
				
				if (selectExpression != null) {
					rightIterator = new SelectIterator(rightIterator, selectExpression);
				}
				
				return new LeftLinearIndexNestedLoopJoinIterator((FromIterator) leftIterator, rightIterator,
						Indexer.indexMapping.get(leftColumn.getWholeColumnName()), anyJoinCondition);
			}
			else if (anyJoinCondition instanceof EqualsTo) {
				selectExpression = getIteratorSpecificCondition(expressionList, leftIterator);
				
				if (selectExpression != null) {
					leftIterator = new SelectIterator(leftIterator, selectExpression);
				}
				
				selectExpression = getIteratorSpecificCondition(expressionList, rightIterator);
				
				if (selectExpression != null) {
					rightIterator = new SelectIterator(rightIterator, selectExpression);
				}
				
				return new InMemorySMJIterator(leftIterator, rightIterator, anyJoinCondition);
			} 
			else {
				expressionList.add(anyJoinCondition);
			}
		}

		return new CrossProductIterator(leftIterator, rightIterator);

	}

	public Expression getIteratorSpecificCondition(List<Expression> expressions, RAIterator iterator) {
		if (expressions.isEmpty()) {
			return null;
		}

		List<Expression> tmp = new ArrayList<Expression>();
		List<Expression> expressionsCopy = new ArrayList<Expression>();

		for (Expression e : expressions) {
			expressionsCopy.add(e);
		}

		for (Expression e : expressionsCopy) {
			if (e instanceof BinaryExpression) {
				BinaryExpression binaryExpression = (BinaryExpression) e;

				if ((binaryExpression.getLeftExpression() instanceof Column
						&& !(binaryExpression.getRightExpression() instanceof Column)
						&& iterator.getIteratorSchema()
								.containsKey(utils.getColumnName((Column) binaryExpression.getLeftExpression())))
						|| (binaryExpression.getRightExpression() instanceof Column
								&& !(binaryExpression.getLeftExpression() instanceof Column)
								&& iterator.getIteratorSchema().containsKey(
										utils.getColumnName((Column) binaryExpression.getRightExpression()))) || ((binaryExpression.getLeftExpression() instanceof Column
												&& (binaryExpression.getRightExpression() instanceof Column)
												&& iterator.getIteratorSchema()
												.containsKey(utils.getColumnName((Column) binaryExpression.getRightExpression()))
												&& iterator.getIteratorSchema()
														.containsKey(utils.getColumnName((Column) binaryExpression.getLeftExpression())))) || recursiveCheckForIteratorSpecificCondition(e, iterator)) {
					expressions.remove(e);
					tmp.add(binaryExpression);
				}
			}
		}

		return mergeAndClauses(tmp);
	}
	
	public boolean recursiveCheckForIteratorSpecificCondition(Expression e, RAIterator iterator) {
		if (e == null)
			return false;
		
		if (!(e instanceof BinaryExpression))
			return false;
		
		BinaryExpression binaryExpression = (BinaryExpression) e;
		
		if ((binaryExpression.getLeftExpression() instanceof Column
				&& !(binaryExpression.getRightExpression() instanceof Column)
				&& iterator.getIteratorSchema()
						.containsKey(utils.getColumnName((Column) binaryExpression.getLeftExpression())))
				|| (binaryExpression.getRightExpression() instanceof Column
						&& !(binaryExpression.getLeftExpression() instanceof Column)
						&& iterator.getIteratorSchema().containsKey(
								utils.getColumnName((Column) binaryExpression.getRightExpression()))) || ((binaryExpression.getLeftExpression() instanceof Column
										&& (binaryExpression.getRightExpression() instanceof Column)
										&& iterator.getIteratorSchema()
										.containsKey(utils.getColumnName((Column) binaryExpression.getRightExpression()))
										&& iterator.getIteratorSchema()
												.containsKey(utils.getColumnName((Column) binaryExpression.getLeftExpression()))))) {
			return true;
		}
		
		return recursiveCheckForIteratorSpecificCondition(binaryExpression.getLeftExpression(), iterator) && recursiveCheckForIteratorSpecificCondition(binaryExpression.getRightExpression(), iterator);
		
	}
}
