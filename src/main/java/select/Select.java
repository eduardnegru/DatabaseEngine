package select;

import database.Symbol;
import database.Table;
import database.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import operation.Operation;
import operation.OperationType;

public class Select
{

	private ExecutorService executorServices;
	private List<String> operations;
	private String condition;
	private Table tableInstance;
	private ConcurrentLinkedQueue<Future<List<Object>>> futures;

	public Select(ExecutorService executorServices, List<String> operations, String condition, Table tableInstance)
	{
		this.executorServices = executorServices;
		this.operations = operations;
		this.condition = condition;
		this.tableInstance = tableInstance;
 		futures = new ConcurrentLinkedQueue<>();
	}

	public List<List<Object>> select() throws Exception
	{
		List<OperationType> operationsRequiringIntegers = new ArrayList<>(Arrays.asList(OperationType.MIN, OperationType.MAX, OperationType.SUM, OperationType.AVG));
		List<List<Object>> selectResult = new ArrayList<>();
		String[] conditionTokens = condition.trim().split(" ");
		String columnName = null, comparator, value = null;
		CountDownLatch latch = new CountDownLatch(operations.size());
		Type columnType = null;
		Symbol symbol = null;

		// the index of the column given in condition if the condition="" then the index is null
		Integer colIndex;

		// parsing condition
		if (conditionTokens.length == 3)
		{
			columnName = conditionTokens[0];
			comparator = conditionTokens[1];
			value = conditionTokens[2];

			if (tableInstance.columnNames.get(columnName) == null)
			{
				throw new Exception("Column name is not valid");
			}

			symbol = Table.getSymbol(comparator);

		}
		else
		{
			if (!condition.equals(""))
			{
				throw new Exception("Number of tokens in condition should be 3");
			}
		}

		if (columnName != null)
		{
			int index = tableInstance.columnNames.get(columnName);
			columnType = tableInstance.columnTypes.get(index);
			Type valueType = tableInstance.getValueType(value);

			if (!valueType.equals(columnType))
			{
				throw new Exception("Value type in condition and column type do not match");
			}
		}
		// parsing operations
		for (int i = 0; i < operations.size(); i++)
		{
			Operation operation = getOperation(operations.get(i));
			String column = operation.getColumn();
			OperationType operationType = operation.getOperationType();

			if (tableInstance.columnTypes.get(tableInstance.columnNames.get(column)) != Type.INT
					&& operationsRequiringIntegers.contains(operationType))
			{
				throw new Exception("Cannot apply " + operationType + " on " + tableInstance.columnTypes.get(tableInstance.columnNames.get(column)));
			}

			if (columnName != null) // there is a condition
			{
				colIndex = tableInstance.columnNames.get(columnName);
			}
			else
			{
				colIndex = null;
			}

			Future<List<Object>> columnValues = executorServices.submit(new SelectThread(tableInstance.values, tableInstance.columnNames.get(column), operationType, value, symbol, columnType, colIndex, latch));
			futures.add(columnValues);
		}

		latch.await();
		int size = futures.size();

		for(int i = 0; i < size; i++)
		{
			List<Object> result = futures.poll().get();
			selectResult.add(result);
		}

		return selectResult;
	}

	private Operation getOperation(String operation) {
		if (operation.contains("(") && operation.contains(")"))
		{
			int openIndex = operation.indexOf("(");
			int endIndex = operation.indexOf(")");
			String operationType = operation.substring(0, openIndex);
			String column = operation.substring(openIndex + 1, endIndex);

			return new Operation(getOperationType(operationType), column);
		}
		else
		{
			return new Operation(null, operation);
		}
	}

	private OperationType getOperationType(String operationType) {
		return OperationType.valueOf(operationType.toUpperCase());
	}
}
