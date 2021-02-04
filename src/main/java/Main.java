import database.Database;
import database.MyDatabase;
import database.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class Main {

	// This is just a test playground.
	public static void main(String[] args) {
		Scanner scanner = new Scanner(System.in);
		MyDatabase db = new Database();
		String[] columnNames = {"name", "grade"};
		Type[] columnTypes = {Type.STRING, Type.INT};

		db.createTable("Students", Arrays.asList(columnNames), Arrays.asList(columnTypes));
		db.initDb(1);

		while (true) {
			String line = scanner.nextLine();
			String []tokens = line.split(" ");

			if (tokens.length < 1)
				continue;

			if (tokens.length == 3 && tokens[0].toLowerCase().equals("insert")) {
				List<Object> values = new ArrayList<>();
				values.add(tokens[1]);
				values.add(Integer.parseInt(tokens[2]));
				System.out.println("Inserting " + values);
				db.insert("Students", values);
			}
			else if (tokens.length > 1 && tokens[0].toLowerCase().equals("select")) {
				List<String> operations = new ArrayList<>();
				List<String> condition = new ArrayList<>();
				boolean whereFound = false;

				for (int i = 1; i < tokens.length; i++) {
					if (!tokens[i].equals("where"))
						if (!whereFound)
							operations.add(tokens[i]);
						else
							condition.add(tokens[i]);

					if (tokens[i].equals("where")) {
						whereFound = true;
					}
				}
				System.out.println("Operations "+ operations);
				System.out.println("Condition "+ String.join(" ", condition));
				List<List<Object>> result = db.select("Students", operations, String.join(" ", condition));
				System.out.println(result);
			}
			else if (tokens.length > 1 && tokens[0].toLowerCase().equals("update")) {
				List<Object> operations = new ArrayList<>();
				List<String> condition = new ArrayList<>();
				boolean whereFound = false;

				for (int i = 1; i < tokens.length; i++) {
					if (!tokens[i].equals("where"))
						if (!whereFound)
							operations.add(tokens[i]);
						else
							condition.add(tokens[i]);

					if (tokens[i].equals("where")) {
						whereFound = true;
					}
				}
				System.out.println("Operations "+ operations);
				System.out.println("Condition "+ String.join(" ", condition));
				db.update("Students", operations, String.join(" ", condition));
			}
			else {
				System.out.println("Wrong format for command " + line);
			}
		}
	}
}
