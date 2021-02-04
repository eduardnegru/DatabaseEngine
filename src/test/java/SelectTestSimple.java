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

public class SelectTestSimple {
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
		value2.add(7);
		db.insert("Students", value2);

		List<Object> value3 = new ArrayList<>();
		value3.add("Adina");
		value3.add(10);
		db.insert("Students", value3);
	}

	@Test
	public void selectWithFilterEquals() {
		String[] operations = {"studentName", "grade"};
		List<List<Object>> results = db.select("Students", Arrays.asList(operations), "grade < 8");
		System.out.println(results);

		assertEquals(5, results.get(1).get(0));
		assertEquals(7, results.get(1).get(1));
	}

	@After
	public void cleanup() {
		db.stopDb();
	}
}
