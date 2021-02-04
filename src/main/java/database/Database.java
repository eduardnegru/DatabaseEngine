package database;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Database implements MyDatabase
{
	private ConcurrentHashMap<String, Table> tablesHashMap = new ConcurrentHashMap<>();
	private ExecutorService executorService;
	public static int numWorkerThreads;

 	@Override
	public void initDb(int numWorkerThreads)
	{
		Database.numWorkerThreads = numWorkerThreads;
		executorService = Executors.newFixedThreadPool(numWorkerThreads);
	}

	@Override
	public void stopDb()
	{
 		executorService.shutdown();
	}

	@Override
	public void createTable(String tableName, List<String> columnNames, List<Type> columnTypes)
	{
		Table table = new Table(tableName, columnNames, columnTypes);
		tablesHashMap.put(tableName, table);
	}

	@Override
	public List<List<Object>> select(String tableName, List<String> operations, String condition)
	{
		Table table = tablesHashMap.get(tableName);
		List<List<Object>> selectedColumns = new ArrayList<>();

   		try
		{
 			selectedColumns = table.select(executorService, operations, condition);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

 		return selectedColumns;
  	}

	@Override
	public void update(String tableName, List<Object> values, String condition)
	{
		Table table = tablesHashMap.get(tableName);

		try
		{
			table.update(executorService, values, condition);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

	}

	@Override
	public void insert(String tableName, List<Object> values)
	{

		Table table =  tablesHashMap.get(tableName);
		try
		{
			table.insert(values);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

	}

	@Override
	public void startTransaction(String tableName)
	{
		Table table = tablesHashMap.get(tableName);
		table.startTransaction();
	}

	@Override
	public void endTransaction(String tableName)
	{
		Table table = tablesHashMap.get(tableName);
		table.stopTransaction();
	}

	public ConcurrentHashMap<String, Table> getTablesHashMap()
	{
		return tablesHashMap;
	}

	@Override
	public String toString()
	{
		return "Database{" +
				"tablesHashMap=" + tablesHashMap +
				", executorService=" + executorService +
				", numWorkerThreads=" + numWorkerThreads +
				'}';
	}
}
