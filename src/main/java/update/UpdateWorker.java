package update;

import database.Symbol;
import database.Table;
import database.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class UpdateWorker implements Runnable
{
	private int startIndex;
	private int endIndex;
 	private List<Object> newValues;
	private Table tableInstance;
	private CountDownLatch countDownLatch;
	private String value;
	private Symbol comparator;
	private Type columnType;
	private int index;

	UpdateWorker(int startIndex, int endIndex, List<Object> newValues, Table tableInstance, CountDownLatch countDownLatch, String value, Symbol comparator, Type columnType, int index)
	{
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		this.newValues = newValues;
		this.tableInstance = tableInstance;
		this.countDownLatch = countDownLatch;
		this.value = value;
		this.comparator = comparator;
		this.columnType = columnType;
		this.index = index;
	}

	@Override
	public void run()
	{
 		for(int i = startIndex; i < endIndex; i++)
		{
			if(index == -1)
			{
				tableInstance.values.set(i, newValues);
 			}
			else if(Table.conditionMatches(tableInstance.values.get(i).get(index), value, comparator, columnType))
			{
				tableInstance.values.set(i, newValues);
			}
		}

		countDownLatch.countDown();
	}
}
