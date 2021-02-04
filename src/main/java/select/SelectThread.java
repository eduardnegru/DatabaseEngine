package select;

import database.Database;
import database.Symbol;
import database.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import operation.OperationType;
import operation.OperationUtils;

public class SelectThread implements Callable<List<Object>>
{
	private int columnIndex;
	private OperationType operationType;
	private List<List<Object>> values;
	private String value;
	private Symbol comparator;
	private Type columnType;
	private Integer conditionColumnIndex;
 	private CountDownLatch latch;


	/**
	 * operationType = null means that there is no operation and that we should select
	 * the entire column from the temporaryTable
	 * @param values
	 * @param columnIndex
	 * @param operationType
	 */

	public SelectThread(List<List<Object>> values, int columnIndex, OperationType operationType, String value, Symbol comparator, Type columnType, Integer conditionColumnIndex, CountDownLatch latch)
	{
		this.columnIndex = columnIndex;
		this.operationType = operationType;
		this.values = values;
		this.value = value;
		this.comparator = comparator;
		this.columnType = columnType;
		this.conditionColumnIndex = conditionColumnIndex;
 		this.latch = latch;
	}

	@Override
	public ArrayList<Object> call() throws Exception
	{
		ArrayList<Object> selectedColumnValues = new ArrayList<>();
		ArrayList<Future<List<Object>>> promises = new ArrayList<>();
		CountDownLatch forLoopLatch = new CountDownLatch(Database.numWorkerThreads);
		ExecutorService executor = Executors.newFixedThreadPool(Database.numWorkerThreads);
		OperationType newOperationType = operationType;

		if (operationType == OperationType.AVG)
			newOperationType = OperationType.SUM;

		for (int i = 0; i < Database.numWorkerThreads; i++)
		{
			int startIndex = i * values.size() / Database.numWorkerThreads;
			int endIndex = (i + 1) * values.size() / Database.numWorkerThreads;

			Future<List<Object>> future =  executor.submit(new ForLoopWorker(startIndex,  endIndex, values, conditionColumnIndex,  newOperationType,  value,  comparator,  columnType,  columnIndex, forLoopLatch));
			promises.add(future);
		}

		forLoopLatch.await();

		for (Future<List<Object>> promise : promises) {
			List<Object> result = promise.get();
			selectedColumnValues.addAll(result);
		}

		if (operationType != null)
		{
			if (operationType == OperationType.COUNT)
			{
				newOperationType = OperationType.SUM;
			}

			Object value = OperationUtils.computeOperation(selectedColumnValues, newOperationType);

			if (value != null && operationType == OperationType.AVG)
			{
				value = (long) value / (double)values.size();
			}

			selectedColumnValues.clear();
			selectedColumnValues.add(value);
		}

		executor.shutdown();

 		latch.countDown();
		return selectedColumnValues;
	}

}