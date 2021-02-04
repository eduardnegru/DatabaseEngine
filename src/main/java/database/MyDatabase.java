package database;

import java.util.List;

public interface MyDatabase {
	void initDb(int numWorkerThreads);
	void stopDb();
	void createTable(String tableName, List<String> columnNames, List<Type> columnTypes);
	List<List<Object>> select(String tableName, List<String> operations, String condition);
	void update(String tableName, List<Object> values, String condition);
	void insert(String tableName, List<Object> values);
	void startTransaction(String tableName);
	void endTransaction(String tableName);
}
