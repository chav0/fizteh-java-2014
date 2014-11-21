package ru.fizteh.fivt.students.alina_chupakhina.storeable.test;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.fizteh.fivt.students.alina_chupakhina.storeable.IncorrectNumberOfArgumentsException;
import ru.fizteh.fivt.students.alina_chupakhina.storeable.Interpreter;
import ru.fizteh.fivt.students.alina_chupakhina.storeable.UnknownCommandException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import static org.junit.Assert.assertEquals;

public class InterpreterTest {
    private final String newLine = System.getProperty("line.separator");
    private ByteArrayOutputStream outputStream;
    private PrintStream printStream;


    @Before
    public void setUp() {
        outputStream = new ByteArrayOutputStream();
        printStream = new PrintStream(outputStream);
    }

    @Test(expected = UnknownCommandException.class)
    public final void testWithInvalidCommandAtTheInvalidCommand() throws Exception {
        Interpreter interpreter = new Interpreter(printStream);
        interpreter.doCommand("command");
    }

    @Test
    public final void testCommandWithoutUsingCurrentTable() throws Exception {
        Interpreter interpreter = new Interpreter(printStream);
        interpreter.doCommand("put 1 1");
        assertEquals("no table" + newLine, outputStream.toString());
    }

    @Test(expected = IncorrectNumberOfArgumentsException.class)
    public final void testWithWrongNumberOfArguments() throws Exception {
        Interpreter interpreter = new Interpreter(printStream);
        interpreter.doCommand("exit 1");
    }

    @Test
    public final void testWithEmptyStringInput() throws Exception {
        Interpreter interpreter = new Interpreter(printStream);
        interpreter.doCommand("");
        assertEquals("", outputStream.toString());
    }

    @Test(expected = IncorrectNumberOfArgumentsException.class)
    public final void testWithWrongNumberOfArgumentsInShowTables() throws Exception {
        Interpreter interpreter = new Interpreter(printStream);
        interpreter.doCommand("show tables 1");
    }

    @Test(expected = Exception.class)
    public final void testInvalidCommandInShowTables() throws Exception {
        Interpreter interpreter = new Interpreter(printStream);
        interpreter.doCommand("show table");
    }

    @Test(expected = Exception.class)
    public final void testInvalidCommandInShowTables2() throws Exception {
        Interpreter interpreter = new Interpreter(printStream);
        interpreter.doCommand("show");
    }

    @Test(expected = Exception.class)
    public final void testCreateEWrongTypesSignature() throws Exception {
        Interpreter interpreter = new Interpreter(printStream);
        interpreter.doCommand("create table (,)");
    }

    @Test(expected = Exception.class)
    public final void testCreateEWrongTypesSignatureEmptyTypes() throws Exception {
        Interpreter interpreter = new Interpreter(printStream);
        interpreter.doCommand("create table (int, ,String)");
    }

    @Test(expected = Exception.class)
    public final void testCreateEWrongTypesSignatureNotValidTypeName() throws Exception {
        Interpreter interpreter = new Interpreter(printStream);
        interpreter.doCommand("create table (test)");
    }

    @Test(expected = Exception.class)
    public final void testCreateEWrongTypesSignatureNoTypes() throws Exception {
        Interpreter interpreter = new Interpreter(printStream);
        interpreter.doCommand("create table (   )");
    }

    @After
    public void tearDown() throws IOException {
        outputStream.close();
        printStream.close();
    }
}