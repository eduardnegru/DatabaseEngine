package database;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import select.Select;
import update.Update;

public class Table
{
	private String name;
	public Map<String, Integer> columnNames = new HashMap<>();
	public Map<Integer, Type> columnTypes = new HashMap<>();
	public List<List<Object>> values = new ArrayList<>();
	private Lock readLock;
	private Lock writeLock;
	private AtomicLong transactionThreadId = new AtomicLong(-1);
	private final Object monitor = new Object();


	Table(String name, List<String> columnNames, List<Type> columnTypes)
	{
		ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
		this.readLock = readWriteLock.readLock();
		this.writeLock = readWriteLock.writeLock();
		this.name = name;

		for(int i = 0; i < columnNames.size(); i++)
		{
			this.columnNames.put(columnNames.get(i), i);
			this.columnTypes.put(i, columnTypes.get(i));
		}
	}

	public List<List<Object>> select(ExecutorService executorServices, List<String> operations, String condition) throws Exception
	{
		if (transactionThreadId.get() != -1 && transactionThreadId.get() != Thread.currentThread().getId())
			synchronized (monitor) {
				monitor.wait();
			}

		readLock.lock();
  		try
		{
 			Select select = new Select(executorServices, operations, condition,this);
			return select.select();
		}
		finally
		{
			readLock.unlock();
		}
	}

	public void insert(List<Object> rowValues)  throws Exception
	{
		if (transactionThreadId.get() != -1 && transactionThreadId.get() != Thread.currentThread().getId())
			synchronized (monitor) {
				monitor.wait();
			}

		writeLock.lock();

		try
		{
			if(rowValues.size() != columnNames.size())
			{
				throw new Exception("Number of table columns and inserted values differ");
			}

			this.values.add(rowValues);
		}
		finally
		{
			writeLock.unlock();
		}

	}

	void update(ExecutorService executorServices, List<Object> newValues, String condition) throws Exception
	{
		if (transactionThreadId.get() != -1 && transactionThreadId.get() != Thread.currentThread().getId())
			synchronized (monitor) {
				monitor.wait();
			}

		writeLock.lock();

		try
		{
			Update update = new Update(executorServices, condition, newValues, this);
			update.update();
		}
		finally
		{
			writeLock.unlock();
		}
	}

	void startTransaction() {
		transactionThreadId.set(Thread.currentThread().getId());
	}

	void stopTransaction()
	{
		transactionThreadId.set(-1);
		synchronized (monitor) {
			monitor.notifyAll();
		}
	}

	public Type getValueType(String value)
	{
		if(value.equals("true") || value.equals("false"))
		{
			return Type.BOOL;
		}
		else if(Character.isDigit(value.charAt(0)))
		{
			return Type.INT;
		}
		else if(value.charAt(0) == '-' && Character.isDigit(value.charAt(1)))
		{
			return Type.INT;
		}

		return Type.STRING;
	}


	public static boolean conditionMatches(Object value1, String value2, Symbol comparator, Type type)
	{
		if(type == null)
		{
			return true;
		}

		// value found in the table
		String convertedValue = value1.toString();

		switch (comparator)
		{
			case EQUALS:
				return  type == Type.INT ? Integer.parseInt(convertedValue) == Integer.parseInt(value2) : convertedValue.compareTo(value2) == 0;
			case LESS_THAN:
				return type == Type.INT ? Integer.parseInt(convertedValue) < Integer.parseInt(value2) :convertedValue.compareTo(value2) > 0;
			case GREATER_THAN:
				return type == Type.INT ? Integer.parseInt(convertedValue) > Integer.parseInt(value2) :convertedValue.compareTo(value2) < 0;
		}


		return false;
	}

	public List<List<Object>> getValues()
	{
		return values;
	}

	public static Symbol getSymbol(String comparator) {
		List<Symbol> symbols = Arrays.asList(Symbol.values());
		List<Symbol> symbolSet = symbols.stream().filter(symbol -> symbol.getSymbol().equals(comparator)).collect(Collectors.toList());
		return symbolSet.size() > 0 ? symbolSet.get(0) : null;
	}

	@Override
	public String toString()
	{
		return "Table{" +
				"name='" + name + '\'' +
				", columnNames=" + columnNames +
				", columnTypes=" + columnTypes +
				", values=" + values +
				'}';
	}
}
