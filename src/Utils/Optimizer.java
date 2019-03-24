package Utils;

import java.util.*;
import Iterators.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class Optimizer {
	public static RAIterator optimizeRA(RAIterator root) {
		if (root instanceof SelectIterator) {
			SelectIterator selectIterator = (SelectIterator) root;

			if (selectIterator.getRightIterator() instanceof CrossProductIterator) {
				CrossProductIterator crossProductIterator = (CrossProductIterator) selectIterator.getRightIterator();
				
				List<Expression> expressionList = splitAndClauses(selectIterator.getExpression());
				Expression equiJoinCondition = getEquiJoinCondition(expressionList, crossProductIterator.getLeftIterator(), crossProductIterator.getRightIterator());
				
				if (equiJoinCondition != null) {
					return new SelectIterator(
							new OnePassHashJoinIterator(optimizeRA(crossProductIterator.getLeftIterator()), optimizeRA(crossProductIterator.getRightIterator()), equiJoinCondition),
							mergeAndClauses(expressionList)
							);
				}
				
				return new SelectIterator(
						new CrossProductIterator(optimizeRA(crossProductIterator.getLeftIterator()), optimizeRA(crossProductIterator.getRightIterator())),
						selectIterator.getExpression()
					);
			}
			
			return new SelectIterator(
					optimizeRA(selectIterator.getRightIterator()), selectIterator.getExpression()
					);
		}
		
		else if(root instanceof ProjectIterator) {
			ProjectIterator projectIterator = (ProjectIterator) root;
			return new ProjectIterator(
					optimizeRA(projectIterator.getRightIterator()), projectIterator.getSelectItems());
		}
		
		else if(root instanceof SubQueryIterator) {
			SubQueryIterator subQueryIterator = (SubQueryIterator) root;
			
			return new SubQueryIterator(
					optimizeRA(subQueryIterator.getRightIterator())
					);
		}
		
		else if (root instanceof UnionIterator) {
			UnionIterator unionIterator = (UnionIterator) root;
			
			List<RAIterator> resultIterators = new ArrayList<RAIterator>();
			for(RAIterator iterator : unionIterator.getIterators()) {
				resultIterators.add(optimizeRA(iterator));
			}
			
			return new UnionIterator(resultIterators);
		}
		
		else if (root instanceof OnePassHashJoinIterator) {
			OnePassHashJoinIterator iterator = (OnePassHashJoinIterator) root;
			
			return new OnePassHashJoinIterator(
						optimizeRA(iterator.getLeftIterator()),
						optimizeRA(iterator.getRightIterator()),
						iterator.getExpression()
					);
		}
		
		else if (root instanceof LimitIterator) {
			LimitIterator iterator = (LimitIterator) root;
			
			return new LimitIterator(
						optimizeRA(iterator.getRightIterator()),
						iterator.getLimit()
					);
		}
		
		else if (root instanceof AggregationIterator) {
			AggregationIterator iterator = (AggregationIterator) root;
			
			return new AggregationIterator(
						optimizeRA(iterator.getRightIterator()),
						iterator.getSelectItems()
					);
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
		
		for(int i = 1; i < expressionList.size(); i++) {
			leftExpression = new AndExpression(leftExpression, expressionList.get(i));
			
		}
		
		return leftExpression;
	}
	
	public static Expression getEquiJoinCondition(List<Expression> expressions, RAIterator leftIterator, RAIterator rightIterator) {
		if (expressions.isEmpty()) {
			return null;
		}
		
		for(Expression e: expressions) {
			if (e instanceof EqualsTo) {
				EqualsTo equalsToExpression = (EqualsTo) e;
				
				if (equalsToExpression.getLeftExpression() instanceof Column && equalsToExpression.getLeftExpression() instanceof Column && 
						leftIterator.getIteratorSchema().containsKey(utils.getColumnName((Column) equalsToExpression.getLeftExpression())) && rightIterator.getIteratorSchema().containsKey(utils.getColumnName((Column) equalsToExpression.getRightExpression()))) {
					expressions.remove(e);
					return e;
				}
			}
		}
		
		return null;
		
	}
}
