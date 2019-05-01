package Utils;

import java.util.*;
import Iterators.*;

import Iterators.RAIterator;
import Models.Schema;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectionPushDown {
	public static RAIterator pushDown(RAIterator root, HashSet<String> columnNames) {
		if (root instanceof ProjectIterator) {
			ProjectIterator iterator = (ProjectIterator) root;
			List<SelectItem> selectItems = iterator.getSelectItems();
			
			for (SelectItem selectItem: selectItems) {
				SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
				Expression expression = selectExpressionItem.getExpression();
				
				utils.getColumnNamesFromExpression(expression, columnNames);
			}
			
			RAIterator resultIterator = pushDown(iterator.getRightIterator(), columnNames);
			iterator.pushDownSchema(resultIterator);
			return iterator;
		} else if (root instanceof FromIterator) {
			FromIterator iterator = (FromIterator) root;
			
			Map<String, Schema> schemaByName = iterator.getIteratorSchema().schemaByName();
			List<String> iteratorSpecificNames = new ArrayList<String>();
			
			for (String colName: columnNames) {
				if (schemaByName.containsKey(colName)) {
					iteratorSpecificNames.add(colName);
				}
			}
			
			for (String colName:iteratorSpecificNames) {
				columnNames.remove(colName);
			}
			
			return new TableIterator(iterator.getTable(), iteratorSpecificNames);
		} else if (root instanceof SelectIterator) {
			SelectIterator iterator = (SelectIterator) root;
			Expression expression = iterator.getExpression();
			utils.getColumnNamesFromExpression(expression, columnNames);
			
			
			RAIterator resultIterator = pushDown(iterator.getRightIterator(), columnNames);
			iterator.pushDownSchema(resultIterator);
			return iterator;
		} else if (root instanceof CrossProductIterator) {
			CrossProductIterator iterator = (CrossProductIterator) root;
			
			
			RAIterator leftResultIterator = pushDown(iterator.getLeftIterator(), columnNames);
			RAIterator rightResultIterator = pushDown(iterator.getRightIterator(), columnNames);
			iterator.pushDownSchema(leftResultIterator, rightResultIterator);
			return iterator;
		} else if (root instanceof LimitIterator) {
			LimitIterator iterator = (LimitIterator) root;
			

			RAIterator rightResultIterator = pushDown(iterator.getRightIterator(), columnNames);
			iterator.pushDownSchema(rightResultIterator);
			return iterator;
		} else if (root instanceof OnePassHashJoinIterator) {
			OnePassHashJoinIterator iterator = (OnePassHashJoinIterator) root;
			Expression expression = iterator.getExpression();
			utils.getColumnNamesFromExpression(expression, columnNames);

			RAIterator leftResultIterator = pushDown(iterator.getLeftIterator(), columnNames);
			RAIterator rightResultIterator = pushDown(iterator.getRightIterator(), columnNames);
			iterator.pushDownSchema(leftResultIterator, rightResultIterator);
			return iterator;
		} else if (root instanceof InMemoryGroupByIterator) {
			InMemoryGroupByIterator iterator = (InMemoryGroupByIterator) root;
			List<SelectItem> selectItems = iterator.getSelectItems();
			
			for (SelectItem selectItem: selectItems) {
				SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
				Expression expression = selectExpressionItem.getExpression();
				
				utils.getColumnNamesFromExpression(expression, columnNames);
			}
			
			RAIterator resultIterator = pushDown(iterator.getRightIterator(), columnNames);
			iterator.pushDownSchema(resultIterator);
			return iterator;
		} else if (root instanceof SortIterator) {
			SortIterator iterator = (SortIterator) root;
			List<OrderByElement> orderByElements = iterator.getOrderByElements();
			
			for (OrderByElement orderByElement: orderByElements) {
				Expression expression = orderByElement.getExpression();
				
				utils.getColumnNamesFromExpression(expression, columnNames);
			}
			
			RAIterator resultIterator = pushDown(iterator.getRightIterator(), columnNames);
			iterator.pushDownSchema(resultIterator);
			return iterator;
		}
		
		return root;
	}
}
