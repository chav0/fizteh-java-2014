package ru.fizteh.fivt.students.alina_chupakhina.parallel;

import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.storage.structured.Storeable;
import ru.fizteh.fivt.storage.structured.Table;
import ru.fizteh.fivt.storage.structured.TableProvider;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MyTableProvider implements TableProvider {

    public static String path;
    public static Map<String, Table> tableList;
    public static MyTable currentTable;
    private TableSerializer ts = new TableSerializer();
    private final ReadWriteLock tableAccessLock = new ReentrantReadWriteLock(true);

    public MyTableProvider(String p) {
        if (p == null) {
            throw new IllegalArgumentException("Directory name is null");
        }
        path = p;
        tableList = new HashMap<>();
        currentTable = null;
    }

    /**
     * Возвращает таблицу с указанным названием.
     *
     * Последовательные вызовы метода с одинаковыми аргументами должны возвращать один и тот же объект таблицы,
     * если он не был удален с помощью {@link #removeTable(String)}.
     *
     * @param name Название таблицы.
     * @return Объект, представляющий таблицу. Если таблицы с указанным именем не существует, возвращает null.
     *
     * @throws IllegalArgumentException Если название таблицы null или имеет недопустимое значение.
     */

    @Override
    public Table getTable(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Directory name is null");
        }
        tableAccessLock.readLock().lock();
        try {
            return Main.tp.tableList.get(name);
        } finally {
            tableAccessLock.readLock().unlock();
        }
    }

    /**
     * Создаёт таблицу с указанным названием.
     * Создает новую таблицу. Совершает необходимые дисковые операции.
     *
     * @param name Название таблицы.
     * @param columnTypes Типы колонок таблицы. Не может быть пустой.
     * @return Объект, представляющий таблицу. Если таблица с указанным именем существует, возвращает null.
     *
     * @throws IllegalArgumentException Если название таблицы null или имеет недопустимое значение. Если список типов
     *                                  колонок null или содержит недопустимые значения.
     * @throws java.io.IOException При ошибках ввода/вывода.
     */

    @Override
    public Table createTable(String name, List<Class<?>> columnTypes) throws IOException {
        if (name == null || columnTypes == null) {
            throw new IllegalArgumentException("Directory name or types of columns is null");
        }
        tableAccessLock.writeLock().lock();
        try {
            String pathToTable = path + File.separator + name;
            File table = new File(pathToTable);
            if (table.exists() && table.isDirectory()) {
                return null;
            } else {
                Table t = new MyTable(name, path, columnTypes);
                if (!table.mkdir()) {
                    System.err.println("Unable to create a table");
                }
                return t;
            }
        } finally {
            tableAccessLock.writeLock().unlock();
        }
    }

    /**
     * Удаляет существующую таблицу с указанным названием.
     *
     * Объект удаленной таблицы, если был кем-то взят с помощью {@link #getTable(String)},
     * с этого момента должен бросать {@link IllegalStateException}.
     *
     * @param name Название таблицы.
     *
     * @throws IllegalArgumentException Если название таблицы null или имеет недопустимое значение.
     * @throws IllegalStateException Если таблицы с указанным названием не существует.
     * @throws java.io.IOException - при ошибках ввода/вывода.
     */

    @Override
    public void removeTable(String name) throws IOException {
        if (name == null) {
            throw new IllegalArgumentException("Name table is null");
        }
        tableAccessLock.writeLock().lock();
        try {
            String pathToTable = Main.tp.path + File.separator + name;
            File table = new File(pathToTable);
            if (!table.exists() || !table.isDirectory()) {
                throw new IllegalStateException("Table not exist");
            } else {
                if (Main.tp.currentTable != null) {
                    if (Main.tp.currentTable.getName().equals(name)) {
                        Main.tp.currentTable = null;
                    }
                }
                MyTable t = (MyTable) Main.tp.tableList.get(name);
                Main.tp.tableList.remove(name);
                t.rm();
                table.delete();
            }
        } finally {
            tableAccessLock.writeLock().unlock();
        }
    }

    @Override
    public Storeable deserialize(Table table, String value) throws ParseException {
        //ts = new TableSerializer();
        return ts.deserialize(table, value, ((MyTable) table).signature);
    }

    @Override
    public String serialize(Table table, Storeable value) throws ColumnFormatException {
        ts = new TableSerializer();
        return ts.serialize(table, value);
    }

    @Override
    public Storeable createFor(Table table) {
        List<Object> lo = new ArrayList<>();
        return new Record(lo, ((MyTable) table).signature);
    }

    @Override
    public Storeable createFor(Table table, List<?> values) throws ColumnFormatException, IndexOutOfBoundsException {
        List<Object> lo = new ArrayList<>(values);
        return new Record(lo, ((MyTable) table).signature);
    }

    @Override
    public List<String> getTableNames() {
        List<String> tablesNames = new ArrayList<>(tableList.keySet());
        return tablesNames;
    }
}
