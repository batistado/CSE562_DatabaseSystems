package Utils;

import java.util.*;
import Iterators.*;
import dubstep.Main;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;

public class Optimizer {
	public static RAIterator optimizeRA(RAIterator root) {
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

			return new SubQueryIterator(optimizeRA(subQueryIterator.getRightIterator()));
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

	public static List<Expression> splitAndClauses(Expression e) {
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

	public static Expression mergeAndClauses(List<Expression> expressionList) {
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

	public static Expression getEquiJoinCondition(List<Expression> expressions, RAIterator leftIterator,
			RAIterator rightIterator) {
		if (expressions.isEmpty()) {
			return null;
		}

		for (Expression e : expressions) {
			if (e instanceof EqualsTo) {
				EqualsTo equalsToExpression = (EqualsTo) e;

				if (equalsToExpression.getLeftExpression() instanceof Column
						&& equalsToExpression.getRightExpression() instanceof Column
						&& leftIterator.getIteratorSchema()
								.containsKey(utils.getColumnName((Column) equalsToExpression.getLeftExpression()))
						&& rightIterator.getIteratorSchema()
								.containsKey(utils.getColumnName((Column) equalsToExpression.getRightExpression()))) {
					expressions.remove(e);
					return e;
				}
			}
		}

		return null;

	}

	public static RAIterator optimizeSelectionOverCross(CrossProductIterator crossProductIterator,
			SelectIterator selectIterator) {
		List<Expression> expressionList = splitAndClauses(selectIterator.getExpression());

		RAIterator iterator = optimizeSelectionOverCrossHelper(crossProductIterator, expressionList);
		Expression expression = mergeAndClauses(expressionList);
		
		return expression == null ? iterator : new SelectIterator(iterator, expression);
	}

	public static RAIterator optimizeSelectionOverCrossHelper(CrossProductIterator crossProductIterator,
			List<Expression> expressionList) {
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
					(CrossProductIterator) crossProductIterator.getRightIterator(), expressionList);

			if (Main.isInMemory) {
				Expression equiJoinCondition = getEquiJoinCondition(expressionList, leftIterator, rightIterator);
	
				if (equiJoinCondition != null) {
					return new OnePassHashJoinIterator(leftIterator, rightIterator, equiJoinCondition);
				}
			}

			return new CrossProductIterator(leftIterator, rightIterator);
		}

		RAIterator rightIterator = crossProductIterator.getRightIterator();
		selectExpression = getIteratorSpecificCondition(expressionList, rightIterator);

		if (selectExpression != null) {
			rightIterator = new SelectIterator(rightIterator, selectExpression);
		}
		
		if (Main.isInMemory) {
			Expression equiJoinCondition = getEquiJoinCondition(expressionList, leftIterator, rightIterator);
	
			if (equiJoinCondition != null) {
				return new OnePassHashJoinIterator(leftIterator, rightIterator, equiJoinCondition);
			}
		}

		return new CrossProductIterator(leftIterator, rightIterator);

	}

	public static Expression getIteratorSpecificCondition(List<Expression> expressions, RAIterator iterator) {
		if (expressions.isEmpty()) {
			return null;
		}

		for (Expression e : expressions) {
			if (e instanceof BinaryExpression) {
				BinaryExpression equalsToExpression = (BinaryExpression) e;

				if ((equalsToExpression.getLeftExpression() instanceof Column
						&& equalsToExpression.getRightExpression() instanceof PrimitiveValue
						&& iterator.getIteratorSchema()
								.containsKey(utils.getColumnName((Column) equalsToExpression.getLeftExpression())))
						|| (equalsToExpression.getRightExpression() instanceof Column
								&& equalsToExpression.getLeftExpression() instanceof PrimitiveValue
								&& iterator.getIteratorSchema().containsKey(
										utils.getColumnName((Column) equalsToExpression.getRightExpression())))) {
					expressions.remove(e);
					return e;
				}
			}
		}

		return null;

	}
}
