package ru.fizteh.fivt.students.alina_chupakhina.storeable;

import ru.fizteh.fivt.storage.structured.Storeable;
import ru.fizteh.fivt.storage.structured.Table;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static ru.fizteh.fivt.students.alina_chupakhina.storeable.TableSerializer.stringToClass;

public class Interpreter {
    public static MyTableProvider tp;
    public PrintStream out;
    public static final String MESSAGE_INVALID_COMMAND = " - invalid command";
    public static final String MESSAGE_INVALID_NUMBER_OF_ARGUMENTS = ": Invalid number of arguments";

    public Interpreter(final PrintStream outStream) {
        out = outStream;
        tp = Main.tp;
    }

    public void doCommand(final String command)
            throws Exception {

        String[] args = command.trim().split("\\s+");
        if (args[0].equals("create")) {
            create(args);
        } else if (args[0].equals("drop")) {
            drop(args);
        } else if (args[0].equals("use")) {
            use(args);
        } else if (args[0].equals("show")) {
            showTables(args);
        } else if (args[0].equals("put")) {
            if (tp.currentTable == null) {
                out.println("no table");
            } else {
                put(args);
            }
        } else if (args[0].equals("get")) {
            if (tp.currentTable == null) {
                out.println("no table");
            } else {
                get(args);
            }
        } else if (args[0].equals("remove")) {
            if (tp.currentTable == null) {
                out.println("no table");
            } else {
                remove(args);
            }
        } else if (args[0].equals("list")) {
            if (tp.currentTable == null) {
                out.println("no table");
            } else {
                list(args);
            }
        } else if (args[0].equals("commit")) {
            if (tp.currentTable == null) {
                out.println("no table");
            } else {
                commit(args);
            }
        } else if (args[0].equals("rollback")) {
            if (tp.currentTable == null) {
                out.println("no table");
            } else {
                rollback(args);
            }
        } else if (args[0].equals("size")) {
            if (tp.currentTable == null) {
                out.println("no table");
            } else {
                size(args);
            }
        } else if (args[0].equals("exit")) {
            exit(args);
        } else if (!args[0].equals("")) {
            throw new UnknownCommandException(args[0] + MESSAGE_INVALID_COMMAND);
        }
    }

    public static void put(final String[] args) throws Exception {
        String storeable = args[2];
        if (args.length > 3) {
            for (int i = 3; i != args.length; i++) {
                storeable = storeable + " " + args[i];
            }
        }
        Storeable value = tp.deserialize(tp.currentTable, storeable);
        if (tp.currentTable.put(args[1], value) == null) {
            System.out.println("new");
        } else {
            System.out.println("overwrite");
        }
    }

    public static void get(final String[] args) throws Exception {
        checkNumOfArgs("get", 2, args.length);
        if (tp.currentTable.get(args[1]) == null) {
            System.out.println("not found");
        } else {
            System.out.println("found");
            System.out.println(tp.serialize(tp.currentTable, tp.currentTable.get(args[1])));
        }
    }

    public static void remove(final String[] args) throws Exception {
        checkNumOfArgs("remove", 2, args.length);
        if (tp.currentTable.remove(args[1]) == null) {
            System.out.println("not found");
        } else {
            System.out.println("removed");
        }
    }

    public static void create(String[] args) throws Exception {
        String typesString = "";
        for (int i = 2; i != args.length; i++) {
            typesString = typesString + " " + args[i];
        }
        typesString = typesString.trim();
        if (typesString.length() < 3
                || typesString.charAt(0) != '('
                || typesString.charAt(typesString.length() - 1) != ')') {
            throw new IllegalArgumentException("wrong types (signature)");
        }
        List<Class<?>> signature = new ArrayList<>();
        String[] types = typesString.substring(1, typesString.length() - 1).split("\\s+");
        for (String type : types) {
            if (type.trim().isEmpty()) {
                throw new Exception("wrong types (signature)");
            }
            Class<?> c = stringToClass(type.trim());
            if (c == null) {
                throw new Exception("wrong type (" + type.trim() + " is not a valid type name)");
            }
            signature.add(c);
        }
        if (types.length == 0) {
            throw new Exception("wrong type (empty type is not allowed)");
        }
        MyTable t = (MyTable)tp.createTable(args[1], signature);
        t.writeSignature();
        if (t != null) {
            System.out.println("created");
            tp.tableList.put(args[1], t);
        } else {
            System.out.println(args[1] + " exists");
        }
    }

    public static void drop(final String[] args) throws Exception {
        checkNumOfArgs("drop", 2, args.length);
        try {
            tp.removeTable(args[1]);
            System.out.println("dropped");
        } catch (IllegalStateException ist) {
            System.out.println(args[1] + " not exists");
        }
    }

    public static void use(String[] args) throws Exception {
        checkNumOfArgs("use", 2, args.length);
        if (tp.tableList.get(args[1]) == null) {
            System.out.println(args[1] + " not exists");
        } else {
            if (tp.currentTable != null && tp.currentTable.unsavedChangesCounter > 0) {
                System.out.println(tp.currentTable.unsavedChangesCounter + " unsaved changes");
            } else {
                tp.currentTable = (MyTable)tp.tableList.get(args[1]);
                System.out.println("using " + args[1]);
            }
        }
    }

    public static void commit(String[] args) throws Exception {
        checkNumOfArgs("commit", 1, args.length);
        System.out.println(tp.currentTable.commit());
    }

    public static void rollback(String[] args) throws Exception {
        checkNumOfArgs("rollback", 1, args.length);
        System.out.println(tp.currentTable.rollback());
    }

    public static void size(String[] args) throws Exception {
        checkNumOfArgs("size", 1, args.length);
        System.out.println(tp.currentTable.numberOfElements);
    }

    public static void list(final String[] args) throws Exception {
        checkNumOfArgs("list", 1, args.length);
        int counter = 0;
        List<String> list = tp.currentTable.list();
        for (String current : list) {
            ++counter;
            System.out.print(current);
            if (counter != list.size()) {
                System.out.print(", ");
            }
        }
        System.out.println();
    }

    public static void showTables(final String[] args) throws Exception {
        if (args.length >= 2) {
            if (!args[1].equals("tables")) {
                throw new Exception("Invalid command");
            }
        }
        if (args.length == 1) {
            throw new Exception("Invalid command");
        }
        checkNumOfArgs("show tables", 2, args.length);
        System.out.println("table_name row_count");
        for (Map.Entry<String, Table> i : Main.tp.tableList.entrySet()) {
            String key = i.getKey();
            int num = ((MyTable) (i.getValue())).numberOfElements;
            System.out.println(key + " " + num);
        }
    }

    public static void exit(final String[] args) throws Exception {
        checkNumOfArgs("exit", 1, args.length);
        System.exit(0);
    }

    public static void checkNumOfArgs(String operation,
                                      int correctValue,
                                      int testValue) throws IncorrectNumberOfArgumentsException {
        if (testValue != correctValue) {
            throw new IncorrectNumberOfArgumentsException(operation
                    + MESSAGE_INVALID_NUMBER_OF_ARGUMENTS);
        }
    }
}
