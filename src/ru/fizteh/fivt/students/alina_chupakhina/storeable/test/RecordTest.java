package ru.fizteh.fivt.students.alina_chupakhina.storeable.test;

import org.junit.Before;
import org.junit.Test;
import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.students.alina_chupakhina.storeable.Record;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class RecordTest {
    private Record test;
    private List<Class<?>> signature;
    private List<Object> values;

    @Before
    public void setUp() {
        values = new ArrayList<>();
        values.add(0);
        values.add("0");
        values.add(false);
        values.add(0.0f);
        signature = new ArrayList<>();
        signature.add(Integer.class);
        signature.add(String.class);
        signature.add(Boolean.class);
        signature.add(Float.class);

        test = new Record(values, signature);
    }

    @Test
    public final void testSetGetColumnAt() throws Exception {
        test.setColumnAt(0, 1);
        assertEquals(1, test.getColumnAt(0));
    }

    @Test
    public final void testGetIntAt() throws Exception {
        assertEquals(new Integer(0), test.getIntAt(0));
    }

    @Test
    public final void testGetBoolAt() throws Exception {
        assertEquals(false, test.getBooleanAt(2));
    }

    @Test
    public final void testGetFloatAt() throws Exception {
        assertEquals(new Float(0.0f), test.getFloatAt(3));
    }

    @Test(expected = ColumnFormatException.class)
    public final void testGetWrongDoubleAt() throws Exception {
        test.getDoubleAt(1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public final void testGetWrongNumOfColumn() throws Exception {
        test.getDoubleAt(4);
    }
}