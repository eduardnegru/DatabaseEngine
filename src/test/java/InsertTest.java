import static org.junit.Assert.assertEquals;
import database.Database;
import database.MyDatabase;
import database.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.Before;
import org.junit.Test;

public class InsertTest {
	private MyDatabase db = new Database();
	private final int rowCount = 10;
	private  int randomRow = new Random().nextInt(rowCount);

	@Before
	public void initialize() {
		String[] columnNames = {"studentName", "grade", "hasGraduated"};
		Type[] columnTypes = {Type.STRING, Type.INT, Type.BOOL};

		db.createTable("Students", Arrays.asList(columnNames), Arrays.asList(columnTypes));

		db.initDb(1);

		for(int i = 0; i < rowCount; i++) {
			ArrayList<Object> values = new ArrayList<>();
			values.add("John" + i);
			values.add(i);
			values.add(i % 2 == 1);
			db.insert("Students", values);
		}

		System.out.println("Random = " + randomRow);
	}

	@Test
	public void insertTest() {

		String[] columnNames = {"studentName", "grade", "hasGraduated"};
		List<List<Object>> results = db.select("Students", Arrays.asList(columnNames), "");

		for (int i = 0; i < rowCount; i++) {
			assertEquals("John" + i, results.get(0).get(i));
			assertEquals(i, results.get(1).get(i));
			assertEquals(i % 2 == 1, results.get(2).get(i));
		}
	}
}
