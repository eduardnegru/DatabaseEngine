import database.Database;
import database.MyDatabase;
import database.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import org.junit.Before;
import org.junit.Test;

public class BigSelectTest {
	private MyDatabase db = new Database();
	private Logger logger = Logger.getLogger(BigSelectTest.class.getName());

	@Before
	public void initialize() {
		String[] columnNames = {"studentName", "grade0", "grade1", "grade2", "grade3"};

		Type[] columnTypes = {Type.STRING, Type.INT, Type.INT, Type.INT, Type.INT};

		db.createTable("Students0", Arrays.asList(columnNames), Arrays.asList(columnTypes));

		db.initDb(4);

		for(int i = 0; i < 1_000_000; i++) {
			ArrayList<Object> values = new ArrayList<>();
			values.add("Ion"+i);
			values.add(i);
			values.add(i);
			values.add(i);
			values.add(i);
			db.insert("Students0", values);
		}
	}

	@Test
	public void selectSum() {
		String[] operations = {"sum(grade0)"};
		List<List<Object>> results = db.select("Students0", Arrays.asList(operations), "grade0 > -1");
		logger.info(results.toString());
	}
}
