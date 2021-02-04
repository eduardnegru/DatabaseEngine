package select;

import database.Symbol;
import database.Table;
import database.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import operation.OperationType;
import operation.OperationUtils;

public class ForLoopWorker implements Callable<List<Object>>
{

	private int startIndex;
	private int endIndex;
	private List<List<Object>> values;
	private Integer conditionColumnIndex;
	private OperationType operationType;
	private String value;
	private Symbol comparator;
	private Type columnType;
	private int columnIndex;
	private CountDownLatch countDownLatch;

	ForLoopWorker(int startIndex, int endIndex, List<List<Object>> values, Integer conditionColumnIndex, OperationType operationType, String value, Symbol comparator, Type columnType, int columnIndex, CountDownLatch countDownLatch)
	{
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		this.values = values;
		this.conditionColumnIndex = conditionColumnIndex;
		this.operationType = operationType;
		this.value = value;
		this.comparator = comparator;
		this.columnType = columnType;
		this.columnIndex = columnIndex;
		this.countDownLatch = countDownLatch;
	}

	@Override
	public List<Object> call()
	{
		List<Object> selectedColumnValues = new ArrayList<>();

		for (int i = startIndex; i < endIndex; i++)
		{
			if (conditionColumnIndex == null || Table.conditionMatches(values.get(i).get(conditionColumnIndex), value, comparator, columnType))
			{
				selectedColumnValues.add(values.get(i).get(columnIndex));
			}
		}

		if(operationType != null)
		{
			Object value = OperationUtils.computeOperation(selectedColumnValues, operationType);
			selectedColumnValues.clear();
			selectedColumnValues.add(value);
		}

		countDownLatch.countDown();
		return selectedColumnValues;
	}
}
