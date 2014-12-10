package ru.fizteh.fivt.students.alina_chupakhina.storeable;

import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.storage.structured.Storeable;
import ru.fizteh.fivt.storage.structured.Table;
import ru.fizteh.fivt.storage.structured.TableProvider;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static ru.fizteh.fivt.students.alina_chupakhina.storeable.TableSerializer.classToString;
import static ru.fizteh.fivt.students.alina_chupakhina.storeable.TableSerializer.stringToClass;

public class MyTable implements Table {

    public TableProvider tp;
    public List<Class<?>> signature;
    private Map<String, Storeable> allRecords;
    final ThreadLocal<Map<String, Storeable>> sessionChanges =
                        ThreadLocal.withInitial(()-> new HashMap<>());
    private ReadWriteLock tableOperationsLock =  new ReentrantReadWriteLock(true);
    //private Map<String, Storeable> sessionChanges;
    public int unsavedChangesCounter;
    public String tableName;
    public String path;
    private File table;
    public int numberOfElements;
    public int columnsCount;
    private static final int NUMBER_OF_FILE = 16;
    private static final int NUMBER_OF_DIR = 16;
    private static final String ENCODING = "UTF-8";


    public MyTable(String name, String pathname, List<Class<?>> columnTypes) {
        tp = Main.tp;
        allRecords = new TreeMap<>();
        //sessionChanges = new TreeMap<>();
        path = pathname + File.separator + name;
        table = new File(path);
        tableName = name;
        unsavedChangesCounter = 0;
        if (columnTypes != null) {
            signature = new ArrayList<>(columnTypes);
            columnsCount = signature.size();
        } else {
            signature = new ArrayList<>();
        }
    }

    /**
     * Устанавливает значение по указанному ключу.
     *
     * @param key Ключ для нового значения. Не может быть null.
     * @param value Новое значение. Не может быть null.
     * @return Значение, которое было записано по этому ключу ранее. Если ранее значения не было записано,
     * возвращает null.
     *
     * @throws IllegalArgumentException Если значение параметров key или value является null.
     * @throws ru.fizteh.fivt.storage.structured.ColumnFormatException - при попытке передать Storeable с колонками другого типа.
     */
    @Override
    public Storeable put(String key, Storeable value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key or value is a null-string");
        }
        tableOperationsLock.readLock().lock();
        try {
            Record s = (Record) sessionChanges.get().put(key, value);
            if (s != null) {
                return s;
            } else {
                unsavedChangesCounter++;
                numberOfElements++;
                return null;
            }
        } finally {
            tableOperationsLock.readLock().unlock();
        }
    }

    /**
     * Удаляет значение по указанному ключу.
     *
     * @param key Ключ для поиска значения. Не может быть null.
     * @return Предыдущее значение. Если не найдено, возвращает null.
     *
     * @throws IllegalArgumentException Если значение параметра key является null.
     */
    @Override
    public Storeable remove(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key is a null-string");
        }
        tableOperationsLock.readLock().lock();
        try {
            Record s = (Record) sessionChanges.get().remove(key);
            if (s != null) {
                unsavedChangesCounter++;
                numberOfElements--;
                return s;
            } else {
                return null;
            }
        } finally {
            tableOperationsLock.readLock().unlock();
        }
    }

    /**
     * Возвращает количество ключей в таблице. Возвращает размер текущей версии, с учётом незафиксированных изменений.
     *
     * @return Количество ключей в таблице.
     */
    public int size() {
        return numberOfElements;
    }

    @Override
    public List<String> list() {
        tableOperationsLock.readLock().lock();
        try {
            List<String> list = new ArrayList<>();
            list.addAll(sessionChanges.get().keySet());
            return list;
        } finally {
            tableOperationsLock.readLock().unlock();
        }
    }

    /**
     * Выполняет фиксацию изменений.
     *
     * @return Число записанных изменений.
     *
     * @throws java.io.IOException если произошла ошибка ввода/вывода. Целостность таблицы не гарантируется.
     */
    public int commit() throws IOException {
        int n;
        tableOperationsLock.writeLock().lock();
        try {
            rm();
            writeSignature();
            try {
                for (Map.Entry<String, Storeable> i : sessionChanges.get().entrySet()) {
                    String key;
                    Record value;
                    key = i.getKey();
                    value = (Record) i.getValue();
                    Integer ndirectory = Math.abs(key.getBytes("UTF-8")[0] % NUMBER_OF_DIR);
                    Integer nfile = Math.abs((key.getBytes("UTF-8")[0] / NUMBER_OF_FILE) % NUMBER_OF_FILE);
                    String pathToDir = path + File.separator + ndirectory.toString()
                            + ".dir";
                    //System.out.println(pathToDir);
                    File file = new File(pathToDir);
                    if (!file.exists()) {
                        file.mkdir();
                    }
                    String pathToFile = path + File.separator + ndirectory.toString()
                            + ".dir" + File.separator + nfile.toString() + ".dat";
                    //System.out.println(pathToFile);
                    file = new File(pathToFile);
                    if (!file.exists()) {
                        file.createNewFile();
                    }
                    DataOutputStream outStream = new DataOutputStream(
                            new FileOutputStream(pathToFile, true));
                    writeValue(outStream, key, value);
                    outStream.close();
                }
            } catch (IOException e) {
                System.err.println(e.getMessage());
                System.exit(-1);
            }
            allRecords = new TreeMap<>(sessionChanges.get());
            n = unsavedChangesCounter;
            unsavedChangesCounter = 0;
        } finally {
            tableOperationsLock.writeLock().unlock();
        }
        return n;
    }

    public void writeValue(DataOutputStream os, String key, Record value) throws IOException {
        byte[] keyBytes = key.getBytes("UTF-8");
        byte[] valueBytes = tp.serialize(this, value).getBytes("UTF-8");
        os.writeInt(keyBytes.length);
        os.write(keyBytes);
        os.writeInt(valueBytes.length);
        os.write(valueBytes);
    }

    public void rm() {

        File[] dirs = this.table.listFiles();
        if (dirs != null) {
            for (File dir : dirs) {
                if (!dir.isDirectory()) {
                    try {
                        if (dir.getName().equals("signature.tsv")) {
                            dir.delete();
                        } else {
                            throw new Exception(dir.getName()
                                    + " is not directory");
                        }
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                        System.exit(-1);
                    }
                }
                if (dir.exists()) {
                    File[] dats = dir.listFiles();
                    if (dats.length == 0) {
                        System.err.println("Empty folders found");
                        System.exit(-1);
                    }
                    for (File dat : dats) {
                        if (!dat.delete()) {
                            System.out.println("Error while reading table " + tableName);
                        }


                    }

                    if (!dir.delete()) {
                        System.out.println("Error while reading table " + tableName);
                    }
                }
            }
        }
    }

    public void load() throws Exception {
        if (!table.exists()) {
            throw new NullPointerException("Directory name is null");
        }
        readSignature();
        tableOperationsLock.writeLock().lock();
        try {
            try {
                File[] dirs = table.listFiles();
                for (File dir : dirs) {
                    if (!dir.isDirectory() && !dir.getName().equals("signature.tsv") && !dir.isHidden()) {
                        System.err.println(dir.getName()
                                + " is not directory");
                        System.exit(-1);
                    }
                    if (!dir.getName().equals("signature.tsv") && !dir.isHidden()) {
                        File[] dats = dir.listFiles();
                        if (dats.length == 0) {
                            System.err.println("Empty folders found");
                            System.exit(-1);
                        }
                        for (File dat : dats) {
                            if (!dat.isHidden()) {
                                int nDirectory = Integer.parseInt(dir.getName().substring(0,
                                        dir.getName().length() - 4));
                                int nFile = Integer.parseInt(dat.getName().substring(0,
                                        dat.getName().length() - 4));
                                String key;
                                Path file = Paths.get(dat.getAbsolutePath());
                                try (DataInputStream fileStream = new DataInputStream(Files.newInputStream(file))) {
                                    while (fileStream.available() > 0) {
                                        key = readKeyValue(fileStream);
                                        if (!(nDirectory == Math.abs(key.getBytes(ENCODING)[0] % NUMBER_OF_DIR))
                                                || !(nFile == Math.abs((key.getBytes(ENCODING)[0]
                                                / NUMBER_OF_FILE) % NUMBER_OF_FILE))) {
                                            System.err.println("Error while reading table " + tableName);
                                            System.exit(-1);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (IOException e) {
                System.err.println(e.getMessage());
                System.exit(-1);
            }
        } finally {
            tableOperationsLock.writeLock().unlock();
        }
        sessionChanges.set(allRecords);
    }

    private String readKeyValue(DataInputStream is) throws Exception {
        int keyLen = is.readInt();
        byte[] keyBytes = new byte[keyLen];
        int keyRead = is.read(keyBytes, 0, keyLen);
        if (keyRead != keyLen) {
            throw new IOException("database: db file is invalid");
        }
        int valLen = is.readInt();
        byte[] valueBytes = new byte[valLen];
        int valRead = is.read(valueBytes, 0, valLen);
        if (valRead != valLen) {
            throw new IOException("database: db file is invalid");
        }

        try {
            String key = new String(keyBytes, "UTF-8");
            Storeable value;
            value = tp.deserialize(this, new String(valueBytes, "UTF-8"));
            allRecords.put(key, value);
            return key;
        } catch (ColumnFormatException e) {
            throw new ColumnFormatException("database: JSON structure is invalid");
        }
    }

    public void writeSignature() throws IOException {
        PrintWriter out = new PrintWriter(table.toPath().resolve("signature.tsv").toString());
        for (Class<?> type : signature) {
            out.print(classToString(type));
            out.print("\t");
        }
        columnsCount = signature.size();
        out.close();
    }

    private void readSignature() throws IOException {
        signature.clear();
        try (BufferedReader reader = Files.newBufferedReader(table.toPath().resolve("signature.tsv"))) {
            String line = reader.readLine();
            for (String token : line.split("\t")) {
                signature.add(stringToClass(token));
            }
        } catch (Exception e) {
            throw new IOException(tableName + ": No signature file or it's empty");
        }
        columnsCount = signature.size();
    }


    /**
     * Выполняет откат изменений с момента последней фиксации.
     *
     * @return Число откаченных изменений.
     */
    public int rollback() {
        int n;
        tableOperationsLock.readLock().lock();
        try {
            sessionChanges.set(allRecords);
            n = unsavedChangesCounter;
            unsavedChangesCounter = 0;
            numberOfElements = allRecords.size();
        } finally {
            tableOperationsLock.readLock().unlock();
        }
        return n;
    }

    /**
     * Возвращает количество колонок в таблице.
     *
     * @return Количество колонок в таблице.
     */
    @Override
    public int getColumnsCount() {
        return columnsCount;
    }

    /**
     * Возвращает тип значений в колонке.
     *
     * @param columnIndex Индекс колонки. Начинается с нуля.
     * @return Класс, представляющий тип значения.
     *
     * @throws IndexOutOfBoundsException - неверный индекс колонки
     */
    @Override
    public Class<?> getColumnType(int columnIndex) throws IndexOutOfBoundsException {
        return signature.get(columnIndex);
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public Storeable get(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key is a null-string");
        }
        tableOperationsLock.readLock().lock();
        try {
            Record s = (Record) sessionChanges.get().get(key);
            return s;
        } finally {
            tableOperationsLock.readLock().unlock();
        }
    }



    /**
     * Возвращает количество изменений, ожидающих фиксации.
     *
     * @return Количество изменений, ожидающих фиксации.
     */
    @Override
    public int getNumberOfUncommittedChanges() {
        return unsavedChangesCounter;
    }
}
