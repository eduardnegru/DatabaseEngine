import static org.junit.Assert.assertEquals;
import database.Symbol;
import database.Table;
import org.junit.Test;

public class SymbolTest {

	@Test
	public void testMin() {
		assertEquals(Symbol.EQUALS, Table.getSymbol("=="));
		assertEquals(Symbol.LESS_THAN, Table.getSymbol("<"));
		assertEquals(Symbol.GREATER_THAN, Table.getSymbol(">"));
	}
}
