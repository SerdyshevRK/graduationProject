package com.itmo;

import java.io.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class KVDataBase {
    // списки объектов для обработки (сохранение)
    Map<Integer, Object> objectsToAdd;

    public KVDataBase() {
        objectsToAdd = new HashMap<>();
    }

    /**
     * помещаем объекты, которые нужно сохранить на дске в коллекцию,
     * которая в последующем будет обрабатываться
     * @param key - задает ключ, который будет использоваться для поиска объекта в базе
     * @param object - объект, который нам нужно сохранить
     */
    public void add(int key, Object object) {
        if (object != null)
            objectsToAdd.put(key, object);
    }

    public void update(int key, Object object) {
        String fileName = object.getClass().getSimpleName() + ".kvdb";    // file with object fields
        String idxFileName = fileName.substring(0, fileName.length() - 5) + "_idx.kvdb";    // file with object key and offset
        try {
            long offset = getOffset(key, idxFileName);
            // todo: mark fields as 'deleted'
        } catch (IOException e) {
            e.printStackTrace();
        }

        add(key,object);
    }

    /**
     * поиск объекта в базе данных по ключу
     * @param key - задает ключ, по которому будем искать объект
     * @param type - тип объекта, который хочет получить пользователь,
     *             позволяет определить в каком файле нужно искать поля для объекта
     * @return возвращает искомый объект или null, если объект не был создан
     */
    public <T> T get(int key, Class<T> type) {
        String fileName = type.getSimpleName() + ".kvdb";     // имя файла из которого читаем поля объекта
        String idxFileName = type.getSimpleName() + "_idx.kvdb";
        T retObj = null;

        try (RandomAccessFile in = new RandomAccessFile(fileName, "r")) {
            retObj = type.newInstance();
            long offset = getOffset(key, idxFileName);
            if (offset == -1)       // если файл существует, но пустой.
                return null;

            in.seek(offset);
            if (key != in.readInt())        // если по смещению лежит другой ключ и другие поля
                return null;
            Field[] fields = type.getDeclaredFields();
            Object value;
            for (Field field : fields) {
                field.setAccessible(true);
                value = readFieldFromFile(field.getType(), in);
                field.set(retObj, value);
            }
        } catch (IOException | IllegalAccessException | InstantiationException e) {
            e.printStackTrace();
        }
        return type.cast(retObj);     // приведение типов
    }

    /**
     * читаем поле из файла и возвращаем его значение пользователю,
     * метод для чтения и возвращаемое значение зависит от типа поля
     * @param type - тип поля, которое надо прочитать
     * @param in - InputStream, из которого читаем значения
     * @param <T> - тип который надо вернуть, задается типом поля, которое читаем
     * @return - возвращаем значение поля из файла или null
     * @throws IOException
     */
    private <T> T readFieldFromFile(Class<T> type, RandomAccessFile in) throws IOException {
        Object retVal = null;
        switch (type.toString()) {
            case "boolean":
                retVal = in.readBoolean();
                break;
            case "int":
                retVal = in.readInt();
                break;
            case "long":
                retVal = in.readLong();
                break;
            case "float":
                retVal = in.readFloat();
                break;
            case "double":
                retVal = in.readDouble();
                break;
            case "class java.lang.String":
                int len = in.readInt();
                byte[] buffer = new byte[len];
                in.read(buffer);
                retVal = new String(buffer);
                break;
        }
        return (T) retVal;
    }

    /**
     * поиск смещения для чтения объекта в основном файле по заданному ключу
     * @param key - ключ объекта, смещение которого мы ищем
     * @param idxFileName - файл, в котором надо искать ключ объекта и его смещение
     * @return - возвращает смещение в основном файле, с которого начинаются поля объекта,
     * или -1, если файл пуст (не содержит ни одной записи о ключах)
     * @throws IOException - выкидывает исключение, если файл не найден
     */
    private long getOffset(int key, String idxFileName) throws IOException {
        File file = new File(idxFileName);
        int keyFromFile;
        long offset = -1;

        if (!file.exists() || !file.isFile()) {
            throw new FileNotFoundException();
        }

        try (RandomAccessFile in = new RandomAccessFile(file, "r")) {
            while (true) {
                try {
                    keyFromFile = in.readInt();
                } catch (EOFException e) {
                    break;
                }

                if (keyFromFile != key) {
                    in.skipBytes(Long.BYTES);         // пропускаем не нужное нам смещение
                    continue;
                }
                offset = in.readLong();     // читаем смещение
                break;
            }
        }

        return offset;
    }

    /* функция для фоновой записи объектов в файл на диске или их удаления с диска*/
    public void save() {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    writeObjectsToFile();
                } catch (IOException | IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        });

        thread.start();
    }

    /**
     * сохранение всех объектов на диск (в БД), записанных в коллекцию 'для сохранения'.
     * считываем имя класса сохраняемого объекта, создаем файл с таким же названием,
     * записываем ключ, назначенный пользователем для этого объекта, в файл,
     * просматриваем все поля в объекте и пишем их в файл после ключа
     * @throws IOException
     * @throws IllegalAccessException
     */
    private void writeObjectsToFile() throws IOException, IllegalAccessException {
        String fileName;
        for (Map.Entry<Integer, Object> entry : objectsToAdd.entrySet()) {
            Class objClass = entry.getValue().getClass();
            fileName = objClass.getSimpleName() + ".kvdb";
            Field[] objFields = objClass.getDeclaredFields();

            writeKeyToFile(entry.getKey(), fileName);

            for (Field field : objFields) {
                Annotation annotation = field.getAnnotation(Exclude.class);
                if (annotation != null) {
                    continue;
                }
                field.setAccessible(true);
                writeFieldToFile(field.getType(), field.get(entry.getValue()), fileName);
            }
        }
        objectsToAdd.clear();
    }

    /**
     * при записи ключа в основной файл, сохраняем смещение, с которого началась запись,
     * и сохраняем его в отдельный файл вместе с ключом для облегчения (ускорения) поиска объекта
     * @param key - ключ объекта, заданный пользователем
     * @param fileName - имя основного файла, в который сохраняются ключ и поля объекта
     * @throws IOException
     */
    private void writeKeyToFile(int key, String fileName) throws IOException {
        String indexFileName = fileName.substring(0, fileName.length() - 5) + "_idx.kvdb";         // имя файла для хранения пар: 'ключ-смещение'
        RandomAccessFile out = null;
        File file = new File(fileName);
        long offset = 0;

        if (file.exists() && file.isFile()) {
            offset = file.length();
        }

        try {
            // сохранение ключа в основном файле
            out = new RandomAccessFile(fileName, "rw");
            out.seek(out.length());
            out.writeInt(key);
        } finally {
            out.close();
        }

        try {
            // сохранение смещения и ключа
            out = new RandomAccessFile(indexFileName, "rw");
            int keyOffset = getKeyOffset(key, indexFileName);
            if (keyOffset < 0) {
                out.seek(out.length());
                out.writeInt(key);
                out.writeLong(offset);
            } else {
                out.seek(keyOffset);
                out.writeInt(key);
                out.writeLong(offset);
            }
        } finally {
            out.close();
        }
    }

    /**
     * проверяем существует ли заданный ключ в файле и, если существует возвращаем его смещение
     * @param key - значение ключа, которое нужно проверить
     * @param idxFileName - имя файла, в котором проверяется наличие ключа
     * @return - возвращаемое значение равно смещению ключа в файле или '-1',
     *          если этого ключа в файле нет, или файл не существует.
     * @throws IOException
     */

    private int getKeyOffset(int key, String idxFileName) throws IOException {
        File file = new File(idxFileName);
        int keyFromFile;
        int offset = -1;
        int currentPosition = 0;

        if (!file.exists() || !file.isFile())
            return offset;

        try (RandomAccessFile in = new RandomAccessFile(file, "r")) {
            while (true) {
                try {
                    keyFromFile = in.readInt();
                } catch (EOFException e) {
                    break;
                }

                if (keyFromFile != key) {
                    currentPosition += Integer.BYTES;
                    currentPosition += in.skipBytes(Long.BYTES);
                } else {
                    offset = currentPosition;
                    break;
                }
            }
        }
        return offset;
    }

    /**
     * запись конкретного поля объекта в основной файл
     * в зависимости от типа поля вызываются разные методы для записи
     * @param type - тип записываемого поля
     * @param data - данные, которые хранятся в этом поле (значение поля: field = value)
     * @param fileName - имя основного файла, в которой это поле будет записано
     * @throws IOException
     */
    private void writeFieldToFile(Class type, Object data, String fileName) throws IOException {
        try(RandomAccessFile out = new RandomAccessFile(fileName, "rw")) {
            out.seek(out.length());
            switch (type.toString()) {
                case "boolean":
                    out.writeBoolean((boolean) data);
                    break;
                case "int":
                    out.writeInt((int) data);
                    break;
                case "long":
                    out.writeLong((long) data);
                    break;
                case "float":
                    out.writeFloat((float) data);
                    break;
                case "double":
                    out.writeDouble((double) data);
                    break;
                case "class java.lang.String":
                    byte[] buffer = ((String) data).getBytes();
                    out.writeInt(buffer.length);
                    out.write(buffer);
                    break;
            }
        }
    }
}
