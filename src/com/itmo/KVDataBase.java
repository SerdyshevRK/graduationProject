package com.itmo;

import java.io.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class KVDataBase {
    private File filesDirectory;    // директория для хранения фалов базы
    private Map<Integer, Object> objectsToAdd;  // список добавленных в базу объектов
    private Map<String, TreeMap<Integer, Long>> primaryKeyMap;  // ключи и смещения сохраненных в базе объектов
    private Map<String, RandomAccessFile> openFiles;    // файлы необходимые базе для работы с данными

    private KVDataBase() {
        objectsToAdd = new HashMap<>();
        openFiles = new TreeMap<>();
        primaryKeyMap = new TreeMap<>();
    }

    /**
     * создаем объект нашей базы, устанавливаем путь к директории, в которой будут храниться или уже храняться данные
     * при начале работы с базой просматриваем директорию,
     * если такой директории не существует (база создается первый раз) создаем ее.
     * если в директории базы находятся файлы, открываем их все на чтение/запись
     * @param directoryPath путь к директории с файлами базы данных
     * @return возвращает созданный объек БД
     */
    public static KVDataBase open(String directoryPath) {
        KVDataBase base = new KVDataBase();

        // create directory for all files needed for data base
        base.filesDirectory = new File(directoryPath);
        if (!base.filesDirectory.exists()) {
            base.filesDirectory.mkdir();
        }

        try {
            base.openAllFilesToRW(base.filesDirectory);
            base.loadKeysToMemory();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return base;
    }

    private void loadKeysToMemory() throws IOException {
        RandomAccessFile in;
        TreeMap<Integer, Long> keyMap;

        for (String fileName : openFiles.keySet()) {
            if (fileName.contains("idx")) {
                in = openFiles.get(fileName);
                keyMap = new TreeMap<>();
                while (true) {
                    try {
                        keyMap.put(in.readInt(), in.readLong());
                        primaryKeyMap.put(fileName, keyMap);
                    } catch (EOFException e) {
                        break;
                    }
                }
            }
        }
    }

    /**
     * закрывает все файлы, которые были открыты при начале работы с базой
     */
    public void close() {
        for (Map.Entry<String, RandomAccessFile> entry : openFiles.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void openAllFilesToRW(File filesDirectory) throws IOException {
        File[] files = filesDirectory.listFiles();
        if (files == null || files.length == 0)
            return;

        RandomAccessFile raf;
        for (File file : files) {
            raf = new RandomAccessFile(file, "rw");
            openFiles.put(file.toString(), raf);
        }
    }

    /**
     * помещаем объекты, которые нужно сохранить на дске в коллекцию,
     * которая в последующем будет обрабатываться
     * @param key задает ключ, который будет использоваться для поиска объекта в базе
     * @param object объект, который нам нужно сохранить
     */
    public void add(int key, Object object) {
        // todo: check if object with this key already exists in base
        if (object != null)
            objectsToAdd.put(key, object);
    }

    /**
     * помечаем объекты, которые надо удалить из базы
     * @param key ключ объекта
     * @param objectType тип удаляемого объекта, определяет в каком файле будет помечаться объект
     */
    public void remove(int key, Class<?> objectType) {
        String fileName = filesDirectory + "\\" + objectType.getSimpleName() + "kvdb";
        try {
            setDeletedMarker(key, fileName, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void deleteObjectsFromFile() {
        // todo: delete keys from idx file
        // todo: delete objects from file ¯\_(ツ)_/¯
    }

    public void update(int key, Object object) {
        String fileName = filesDirectory + "\\" + object.getClass().getSimpleName() + ".kvdb";    // file with object fields
        try {
            setDeletedMarker(key, fileName, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        add(key,object);
    }

    /**
     * поиск объекта в базе данных по ключу
     * @param key задает ключ, по которому будем искать объект
     * @param type тип объекта, который хочет получить пользователь,
     *             позволяет определить в каком файле нужно искать поля для объекта
     * @return возвращает искомый объект или null, если объект не был создан
     */
    public <T> T get(int key, Class<T> type) {
        String fileName = filesDirectory + "\\" +  type.getSimpleName() + ".kvdb";     // имя файла из которого читаем поля объекта
        String idxFileName = fileName.substring(0, fileName.length() - 5) + "_idx.kvdb";
        T retObj = null;

        try {
            RandomAccessFile raf = openFiles.get(fileName);
            if (raf == null)
                return null;

            retObj = type.newInstance();
            long offset = getOffset(key, idxFileName);
            if (offset == -1)   // если файл существует, но пустой.
                return null;

            raf.seek(offset);
            if (key != raf.readInt())    // если по смещению лежит другой ключ и другие поля
                return null;
            raf.skipBytes(Byte.BYTES);   // пропускаем флаг удаления
            Field[] fields = type.getDeclaredFields();
            Object value;
            for (Field field : fields) {
                field.setAccessible(true);
                value = readFieldFromFile(field.getType(), raf);
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
     * @param type тип поля, которое надо прочитать
     * @param raf InputStream, из которого читаем значения
     * @param <T> тип который надо вернуть, задается типом поля, которое читаем
     * @return возвращаем значение поля из файла или null
     * @throws IOException
     */
    private <T> T readFieldFromFile(Class<T> type, RandomAccessFile raf) throws IOException {
        Object retVal = null;
        switch (type.toString()) {
            case "boolean":
                retVal = raf.readBoolean();
                break;
            case "int":
                retVal = raf.readInt();
                break;
            case "long":
                retVal = raf.readLong();
                break;
            case "float":
                retVal = raf.readFloat();
                break;
            case "double":
                retVal = raf.readDouble();
                break;
            case "class java.lang.String":
                int len = raf.readInt();
                byte[] buffer = new byte[len];
                raf.read(buffer);
                retVal = new String(buffer);
                break;
        }
        return (T) retVal;
    }

    /**
     * поиск смещения для чтения объекта в основном файле по заданному ключу
     * @param key ключ объекта, смещение которого мы ищем
     * @param idxFileName файл, в котором надо искать ключ объекта и его смещение
     * @return возвращает смещение в основном файле, с которого начинаются поля объекта,
     * или -1, если файл с индексами пуст (не содержит ни одной записи о ключах)
     */
    private long getOffset(int key, String idxFileName) {
        Map<Integer, Long> map;
        map = primaryKeyMap.get(idxFileName);

        return map.get(key) == null ? -1 : map.get(key);
    }

    /* функция записи объектов в файл, обновления данных*/
    public void save() {
        try {
            writeObjectsToFile();
        } catch (IOException | IllegalAccessException e) {
            e.printStackTrace();
        }
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
            fileName = filesDirectory + "\\" +  objClass.getSimpleName() + ".kvdb";
            Field[] objFields = objClass.getDeclaredFields();

            writeKeyToFile(entry.getKey(), fileName);
            setDeletedMarker(entry.getKey(), fileName, false);

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
     * поднимает флаг, который сообщает о том, что объект (поля объекта) должны быть удалены из файла
     * @param key ключ объекта который помечается как удаленный
     * @param fileName имя файла, в котором записаны поля объекта
     * @param isDeleted определяет нужно ли удалить этот объект или нет
     * @throws IOException
     */
    private void setDeletedMarker(int key, String fileName, boolean isDeleted) throws IOException {
        String idxFileName = fileName.substring(0, fileName.length() - 5) + "_idx.kvdb";
        long offset = getOffset(key, idxFileName);
        RandomAccessFile raf = openFiles.get(fileName);
        raf.seek(offset);
        raf.skipBytes(Integer.BYTES);   // пропускаем ключ
        raf.writeBoolean(isDeleted);
    }

    /**
     * при записи ключа в основной файл, сохраняем смещение, с которого началась запись,
     * и сохраняем его в отдельный файл вместе с ключом для облегчения (ускорения) поиска объекта
     * @param key ключ объекта, заданный пользователем
     * @param fileName имя основного файла, в который сохраняются ключ и поля объекта
     * @throws IOException
     */
    private void writeKeyToFile(int key, String fileName) throws IOException {
        String idxFileName = fileName.substring(0, fileName.length() - 5) + "_idx.kvdb";  // имя файла для хранения пар: 'ключ-смещение'
        RandomAccessFile raf;
        long offset = 0;

        // сохранение ключа в основном файле
        raf = openFiles.get(fileName);
        if (raf == null) {
            raf = new RandomAccessFile(fileName, "rw");
            openFiles.put(fileName, raf);
        }

        if (raf != null)
            offset = raf.length();

        raf.seek(raf.length());
        raf.writeInt(key);

        // сохранение смещения и ключа
        raf = openFiles.get(idxFileName);
        if (raf == null) {
            raf = new RandomAccessFile(idxFileName, "rw");
            openFiles.put(idxFileName, raf);
        }

        int keyOffset = getKeyOffset(key, idxFileName);
        if (keyOffset < 0) {
            raf.seek(raf.length());
            raf.writeInt(key);
            raf.writeLong(offset);
        } else {
            raf.seek(keyOffset);
            raf.writeInt(key);
            raf.writeLong(offset);
        }

        addKeyToMemory(key, offset, idxFileName);
    }

    /**
     * добавляет новый ключ с его смещением, либо обновляет уже имеющееся значение в памяти
     * @param key ключ, который надо добавить или обновить
     * @param offset значение смещения этого ключа
     * @param idxFileName файл, в который он будет записан
     */
    private void addKeyToMemory(int key, long offset, String idxFileName) {
        TreeMap<Integer, Long> map = primaryKeyMap.get(idxFileName);
        if (map == null) {
            map = new TreeMap<>();
            map.put(key, offset);
            primaryKeyMap.put(idxFileName, map);
            return;
        }

        map.put(key, offset);
    }

    /**
     * проверяем существует ли заданный ключ в файле и, если существует возвращаем его смещение
     * @param key значение ключа, которое нужно проверить
     * @param idxFileName имя файла, в котором проверяется наличие ключа
     * @return возвращаемое значение равно смещению ключа в файле или '-1',
     *          если этого ключа в файле нет, или файл не существует.
     * @throws IOException
     */

    private int getKeyOffset(int key, String idxFileName) throws IOException {
        int keyFromFile;
        int offset = -1;
        int currentPosition = 0;

        RandomAccessFile raf = openFiles.get(idxFileName);
        if (raf == null) {
            return offset;
        }

        while (true) {
            try {
                keyFromFile = raf.readInt();
            } catch (EOFException e) {
                break;
            }

            if (keyFromFile != key) {
                currentPosition += Integer.BYTES;
                currentPosition += raf.skipBytes(Long.BYTES);
            } else {
                offset = currentPosition;
                break;
            }
        }

        return offset;
    }

    /**
     * запись конкретного поля объекта в основной файл
     * в зависимости от типа поля вызываются разные методы для записи
     * @param type тип записываемого поля
     * @param data данные, которые хранятся в этом поле (значение поля: field = value)
     * @param fileName имя основного файла, в которой это поле будет записано
     * @throws IOException
     */
    private void writeFieldToFile(Class type, Object data, String fileName) throws IOException {
        RandomAccessFile raf = openFiles.get(fileName);

        raf.seek(raf.length());
        switch (type.toString()) {
            case "boolean":
                raf.writeBoolean((boolean) data);
                break;
            case "int":
                raf.writeInt((int) data);
                break;
            case "long":
                raf.writeLong((long) data);
                break;
            case "float":
                raf.writeFloat((float) data);
                break;
            case "double":
                raf.writeDouble((double) data);
                break;
            case "class java.lang.String":
                byte[] buffer = ((String) data).getBytes();
                raf.writeInt(buffer.length);
                raf.write(buffer);
                break;
        }
    }
}
