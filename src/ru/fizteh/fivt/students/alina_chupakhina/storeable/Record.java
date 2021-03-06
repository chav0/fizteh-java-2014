package ru.fizteh.fivt.students.alina_chupakhina.storeable;

import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.storage.structured.Storeable;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by opa on 16.11.2014.
 */
public class Record implements Storeable {
    public List<Class<?>> valuesTypes;
    private List<Object> values;
    private int columnsNum;

    public Record(List<Object> v, List<Class<?>> vt) {
        values = new ArrayList<>(v);
        columnsNum = values.size();
        valuesTypes = new ArrayList<>(vt);
    }

    /**
     * Установить значение в колонку
     * @param columnIndex - индекс колонки в таблице, начиная с нуля
     * @param value - значение, которое нужно установить.
     *              Может быть null.
     *              Тип значения должен соответствовать декларированному типу колонки.
     * @throws ru.fizteh.fivt.storage.structured.ColumnFormatException - Тип значения не соответствует типу колонки.
     * @throws IndexOutOfBoundsException - Неверный индекс колонки.
     */
    @Override
    public void setColumnAt(int columnIndex, Object value) throws ColumnFormatException, IndexOutOfBoundsException {
        if (columnIndex > columnsNum) {
            throw new IndexOutOfBoundsException("This column does not exist");
        }
        if (value.getClass() != valuesTypes.get(columnIndex)) {
            throw new ColumnFormatException("Invalid column format: expected "
                    + values.get(columnIndex).getClass().getName() + ", got " + value.getClass().getName());
        }
        values.set(columnIndex, value);
    }

    /**
     * Возвращает значение из данной колонки, не приводя его к конкретному типу.
     * @param columnIndex - индекс колонки в таблице, начиная с нуля
     * @return - значение в этой колонке, без приведения типа. Может быть null.
     * @throws IndexOutOfBoundsException - Неверный индекс колонки.
     */
    @Override
    public Object getColumnAt(int columnIndex) throws IndexOutOfBoundsException {
        if (columnIndex > columnsNum) {
            throw new IndexOutOfBoundsException("This column does not exist");
        }
        return values.get(columnIndex);
    }

    /**
     * Возвращает значение из данной колонки, приведя его к Integer.
     * @param columnIndex - индекс колонки в таблице, начиная с нуля
     * @return - значение в этой колонке, приведенное к Integer. Может быть null.
     * @throws ColumnFormatException - Запрошенный тип не соответствует типу колонки.
     * @throws IndexOutOfBoundsException - Неверный индекс колонки.
     */
    public Integer getIntAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        if (columnIndex > columnsNum) {
            throw new IndexOutOfBoundsException("This column does not exist");
        }
        if (!(values.get(columnIndex) instanceof Integer)) {
            throw new ColumnFormatException("Column is not Integer");
        }
        return (Integer) values.get(columnIndex);
    }

    /**
     * Возвращает значение из данной колонки, приведя его к Long.
     * @param columnIndex - индекс колонки в таблице, начиная с нуля
     * @return - значение в этой колонке, приведенное к Long. Может быть null.
     * @throws ColumnFormatException - Запрошенный тип не соответствует типу колонки.
     * @throws IndexOutOfBoundsException - Неверный индекс колонки.
     */
    public Long getLongAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        if (columnIndex > columnsNum) {
            throw new IndexOutOfBoundsException("This column does not exist");
        }
        if (!(values.get(columnIndex) instanceof Long)) {
            throw new ColumnFormatException("Column is not Long");
        }
        return (Long) values.get(columnIndex);
    }

    /**
     * Возвращает значение из данной колонки, приведя его к Byte.
     * @param columnIndex - индекс колонки в таблице, начиная с нуля
     * @return - значение в этой колонке, приведенное к Byte. Может быть null.
     * @throws ColumnFormatException - Запрошенный тип не соответствует типу колонки.
     * @throws IndexOutOfBoundsException - Неверный индекс колонки.
     */
    public Byte getByteAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        if (columnIndex > columnsNum) {
            throw new IndexOutOfBoundsException("This column does not exist");
        }
        if (!(values.get(columnIndex) instanceof Byte)) {
            throw new ColumnFormatException("Column is not Byte");
        }
        return (Byte) values.get(columnIndex);
    }

    /**
     * Возвращает значение из данной колонки, приведя его к Float.
     * @param columnIndex - индекс колонки в таблице, начиная с нуля
     * @return - значение в этой колонке, приведенное к Float. Может быть null.
     * @throws ColumnFormatException - Запрошенный тип не соответствует типу колонки.
     * @throws IndexOutOfBoundsException - Неверный индекс колонки.
     */
    public Float getFloatAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        if (columnIndex > columnsNum) {
            throw new IndexOutOfBoundsException("This column does not exist");
        }
        if (!(values.get(columnIndex) instanceof Float)) {
            throw new ColumnFormatException("Column is not Float");
        }
        return (Float) values.get(columnIndex);
    }

    /**
     * Возвращает значение из данной колонки, приведя его к Double.
     * @param columnIndex - индекс колонки в таблице, начиная с нуля
     * @return - значение в этой колонке, приведенное к Double. Может быть null.
     * @throws ColumnFormatException - Запрошенный тип не соответствует типу колонки.
     * @throws IndexOutOfBoundsException - Неверный индекс колонки.
     */
    public Double getDoubleAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        if (columnIndex > columnsNum) {
            throw new IndexOutOfBoundsException("This column does not exist");
        }
        if (!(values.get(columnIndex) instanceof Double)) {
            throw new ColumnFormatException("Column is not Double");
        }
        return (Double) values.get(columnIndex);
    }

    /**
     * Возвращает значение из данной колонки, приведя его к Boolean.
     * @param columnIndex - индекс колонки в таблице, начиная с нуля
     * @return - значение в этой колонке, приведенное к Boolean. Может быть null.
     * @throws ColumnFormatException - Запрошенный тип не соответствует типу колонки.
     * @throws IndexOutOfBoundsException - Неверный индекс колонки.
     */
    public Boolean getBooleanAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        if (columnIndex > columnsNum) {
            throw new IndexOutOfBoundsException("This column does not exist");
        }
        if (!(values.get(columnIndex) instanceof Boolean)) {
            throw new ColumnFormatException("Column is not Boolean");
        }
        return (Boolean) values.get(columnIndex);
    }

    /**
     * Возвращает значение из данной колонки, приведя его к String.
     * @param columnIndex - индекс колонки в таблице, начиная с нуля
     * @return - значение в этой колонке, приведенное к String. Может быть null.
     * @throws ColumnFormatException - Запрошенный тип не соответствует типу колонки.
     * @throws IndexOutOfBoundsException - Неверный индекс колонки.
     */
    public String getStringAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        if (columnIndex > columnsNum) {
            throw new IndexOutOfBoundsException("This column does not exist");
        }
        if (!(values.get(columnIndex) instanceof String)) {
            throw new ColumnFormatException("Column is not String");
        }
        return (String) values.get(columnIndex);
    }

    public int getColumnsNum() {
        return columnsNum;
    }
}
