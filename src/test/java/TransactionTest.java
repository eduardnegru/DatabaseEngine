import static org.junit.Assert.assertEquals;
import database.Database;
import database.MyDatabase;
import database.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TransactionTest {
	private MyDatabase db = new Database();

	@Before
	public void initialize() {
		String[] columnNames = {"studentName", "grade"};
		Type[] columnTypes = {Type.STRING, Type.INT};

		db.createTable("Students", Arrays.asList(columnNames), Arrays.asList(columnTypes));

		db.initDb(1);


		List<Object> values = new ArrayList<>();
		values.add("Ana");
		values.add(5);
		db.insert("Students", values);

		List<Object> value2 = new ArrayList<>();
		value2.add("Maria");
		value2.add(6);
		db.insert("Students", value2);

		List<Object> value3 = new ArrayList<>();
		value3.add("Adina");
		value3.add(7);
		db.insert("Students", value3);
	}

	@Test
	public void selectWithFilterEquals() throws InterruptedException {


		Thread tInsert = new Thread(() -> {
			db.startTransaction("Students");

			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			List<Object> value3 = new ArrayList<>();
			value3.add("Clara");
			value3.add(8);
			db.insert("Students", value3);

			List<Object> value4 = new ArrayList<>();
			value4.add("Ioana");
			value4.add(8);
			db.insert("Students", value4);


			db.endTransaction("Students");

		});

		Thread tSelect = new Thread(() -> {
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			String[] operations = {"studentName", "grade"};
			List<List<Object>> results = db.select("Students", Arrays.asList(operations), "");
			System.out.println(results);
			assertEquals(2, results.size());
			assertEquals(5, results.get(0).size());
		});

		tInsert.start();
		tSelect.start();

		tInsert.join();
		tSelect.join();
	}

	@After
	public void cleanup() {
		db.stopDb();
	}
}
