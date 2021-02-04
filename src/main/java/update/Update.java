package update;

import database.Database;
import database.Symbol;
import database.Table;
import database.Type;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

public class Update
{
	private ExecutorService executorServices;
	private String condition;
	private List<Object> newValues;
	private Table tableInstance;

	public Update(ExecutorService executorServices, String condition, List<Object> newValues, Table tableInstance)
	{
		this.executorServices = executorServices;
		this.condition = condition;
		this.newValues = newValues;
		this.tableInstance = tableInstance;
	}

	public void update() throws Exception
	{
		int index = -1;
		Type columnType = null;
		String[] conditionTokens = condition.trim().split(" ");
		String columnName = null, comparator, value = null;
		Symbol symbol = null;

		if(conditionTokens.length == 3) /// checking condition
		{
			columnName = conditionTokens[0];
			comparator = conditionTokens[1];
			value = conditionTokens[2];

			if(tableInstance.columnNames.get(columnName) == null)
			{
				throw new Exception("Column name is not valid");
			}

			symbol = Table.getSymbol(comparator);
		}
		else
		{
			if(!condition.equals(""))
			{
				throw new Exception("Number of tokens in condition should be 3");
			}
		}

		// parsing condition
		if(columnName != null)
		{
			index = tableInstance.columnNames.get(columnName);
			columnType = tableInstance.columnTypes.get(index);
			Type valueType = tableInstance.getValueType(value);

			if(!valueType.equals(columnType))
			{
				throw new Exception("Value type and column type do not match");
			}
		}
		CountDownLatch countDownLatch = new CountDownLatch(Database.numWorkerThreads);

		int size = tableInstance.values.size();

		for (int i = 0; i < Database.numWorkerThreads; i++)
		{
			executorServices.submit(new UpdateWorker(i * size / Database.numWorkerThreads, (i + 1) * size / Database.numWorkerThreads, newValues, tableInstance, countDownLatch, value, symbol, columnType, index));
		}

		countDownLatch.await();
	}
}
