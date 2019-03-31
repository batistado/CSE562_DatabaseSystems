package Utils;

import java.util.*;
import Iterators.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;

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

		else {
			return root;
		}

	}

	public List<Expression> splitAndClauses(Expression e) {
		List<Expression> splitExpressions = new ArrayList<Expression>();

		if (e instanceof AndExpression) {
			AndExpression a = (AndExpression) e;
			splitExpressions.addAll(splitAndClauses(a.getLeftExpression()));
			splitExpressions.addAll(splitAndClauses(a.getRightExpression()));
		} else {
			splitExpressions.add(e);
		}

		return splitExpressions;
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
						&& rightIterator.getIteratorSchema()
								.containsKey(utils.getColumnName((Column) equalsToExpression.getRightExpression()))) || (leftIterator.getIteratorSchema()
										.containsKey(utils.getColumnName((Column) equalsToExpression.getRightExpression()))
										&& rightIterator.getIteratorSchema()
												.containsKey(utils.getColumnName((Column) equalsToExpression.getLeftExpression()))))) {
					
					
					expressions.remove(e);
					
					if ((leftIterator.getIteratorSchema()
										.containsKey(utils.getColumnName((Column) equalsToExpression.getRightExpression()))
										&& rightIterator.getIteratorSchema()
												.containsKey(utils.getColumnName((Column) equalsToExpression.getLeftExpression())))) {
						return new EqualsTo(equalsToExpression.getRightExpression(), equalsToExpression.getLeftExpression());
					}
					
					
					return e;
				}
			}
		}

		return null;

	}

	public RAIterator optimizeSelectionOverCross(CrossProductIterator crossProductIterator,
			SelectIterator selectIterator) {
		List<Expression> expressionList = splitAndClauses(selectIterator.getExpression());

		RAIterator iterator = optimizeSelectionOverCrossHelper(crossProductIterator, expressionList, 0);
		Expression expression = mergeAndClauses(expressionList);
		
		return expression == null ? iterator : new SelectIterator(iterator, expression);
	}

	public RAIterator optimizeSelectionOverCrossHelper(CrossProductIterator crossProductIterator,
			List<Expression> expressionList, Integer level) {
		if (expressionList.isEmpty()) {
			return crossProductIterator;
		}

		RAIterator leftIterator = crossProductIterator.getLeftIterator();
		Expression selectExpression = getIteratorSpecificCondition(expressionList, leftIterator);

		if (selectExpression != null) {
			leftIterator = new SelectIterator(leftIterator, selectExpression);
		}

		if (crossProductIterator.getRightIterator() instanceof CrossProductIterator) {
			RAIterator rightIterator = optimizeSelectionOverCrossHelper(
					(CrossProductIterator) crossProductIterator.getRightIterator(), expressionList, level + 1);

//			if (Main.isInMemory) {
				Expression equiJoinCondition = getEquiJoinCondition(expressionList, leftIterator, rightIterator);
	
				if (equiJoinCondition != null) {
					
					if (level == 0) {
						return new OnePassHashJoinIterator(leftIterator, rightIterator, equiJoinCondition);
					} else {
						return new SortMergeJoinIterator(leftIterator, rightIterator, equiJoinCondition);
					}
				}
//			}

			return new CrossProductIterator(leftIterator, rightIterator);
		}

		RAIterator rightIterator = crossProductIterator.getRightIterator();
		selectExpression = getIteratorSpecificCondition(expressionList, rightIterator);

		if (selectExpression != null) {
			rightIterator = new SelectIterator(rightIterator, selectExpression);
		}
		
//		if (Main.isInMemory) {
			Expression equiJoinCondition = getEquiJoinCondition(expressionList, leftIterator, rightIterator);
	
			if (equiJoinCondition != null) {
				if (level == 0) {
					return new OnePassHashJoinIterator(leftIterator, rightIterator, equiJoinCondition);
				} else {
					return new SortMergeJoinIterator(leftIterator, rightIterator, equiJoinCondition);
				}
			}
//		}

		return new CrossProductIterator(leftIterator, rightIterator);

	}

	public Expression getIteratorSpecificCondition(List<Expression> expressions, RAIterator iterator) {
		if (expressions.isEmpty()) {
			return null;
		}
		
		
		List<Expression> tmp = new ArrayList<Expression>();
		List<Expression> expressionsCopy = new ArrayList<Expression>();
		
		for(Expression e: expressions) {
			expressionsCopy.add(e);
		}

		for (Expression e : expressionsCopy) {
			if (e instanceof BinaryExpression) {
				BinaryExpression binaryExpression = (BinaryExpression) e;

				if ((binaryExpression.getLeftExpression() instanceof Column
						&& ! (binaryExpression.getRightExpression() instanceof Column)
						&& iterator.getIteratorSchema()
								.containsKey(utils.getColumnName((Column) binaryExpression.getLeftExpression())))
						|| (binaryExpression.getRightExpression() instanceof Column
								&& ! (binaryExpression.getLeftExpression() instanceof Column)
								&& iterator.getIteratorSchema().containsKey(
										utils.getColumnName((Column) binaryExpression.getRightExpression())))) {
					expressions.remove(e);
					tmp.add(binaryExpression);
				}
			}
		}
		
		return mergeAndClauses(tmp);
	}
}
