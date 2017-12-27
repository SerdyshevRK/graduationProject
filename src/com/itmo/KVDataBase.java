package com.itmo;

import java.io.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class KVDataBase {
    private String mainDirectory;
    private final String extension = ".kvdb";
    private HashMap<Path, HashMap<Integer, Long>> keys;
    private HashMap<Path, Boolean> rewriteStatus;
    private HashMap<String, String> savedTypes;

    private KVDataBase() {
        keys = new HashMap<>();
        rewriteStatus = new HashMap<>();
        savedTypes = new HashMap<>();
    }

    /**
     * создает объект KVDataBase, передает ему путь к директории, где лежат файлы с данными (указывается пользователем).
     * если такой директории не существует, создает ее
     * читает key-файлы в память, если они присутствуют в директории
     * @param directoryPath путь к директории с файлами
     * @return возвращает новый обект KVDataBase
     */
    public static KVDataBase open(String directoryPath) {
        KVDataBase dataBase = new KVDataBase();
        dataBase.mainDirectory = directoryPath;
        File directory = new File(dataBase.mainDirectory);
        if (!directory.exists())
            directory.mkdir();

        dataBase.readKeyFiles();
        dataBase.readTypesFromFile();

        System.out.println("LOG: " + dataBase.keys.toString());

        return dataBase;
    }

    /**
     * просматривает имена файлов и сохраняет их в мапу со статусом перезаписи (true/false)
     * читает данные из файлов с ключами, сохраняет их в мапу 'путь - мапа ключей'
     */
    private void readKeyFiles() {
        HashMap<Integer, Long> keyOffsetMap;

        try (DirectoryStream<Path> directory = Files.newDirectoryStream(Paths.get(mainDirectory))) {
            for (Path file : directory) {
                rewriteStatus.put(file, false);
                if (file.getFileName().toString().contains("Keys")) {
                    keyOffsetMap = readKeysAndOffsetsFromFile(file);
                    keys.put(file, keyOffsetMap);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("LOG: фалы с ключами прочитаны.");
    }

    /**
     * читает значения ключей и смещений из файла
     * @param filePath путь к файлу из которого читаются значения
     * @return возвращает мапу 'ключ-смещение' или null, если в файле не было записей
     */
    private HashMap<Integer, Long> readKeysAndOffsetsFromFile(Path filePath) {
        HashMap<Integer, Long> keyOffsetMap = new HashMap<>();
        int key;
        long offset;

        try(RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "rw")) {
            while (true) {
                try {
                    key = raf.readInt();
                    offset = raf.readLong();
                    keyOffsetMap.put(key, offset);
                } catch (EOFException e) {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("LOG: файл с ключами и смещениями прочитан.");
        return keyOffsetMap;
    }

    /**
     * добавляет объект в базу данных: сохраняет значение его полей в файл
     * сохраняет его смещение по ключу в файле с ключами
     * @param key ключ для дальнейшего поиска объекта
     * @param object объект, который надо добавить в базу данных
     */
    public void add(int key, Object object) {
        long offset;
        Class<?> clazz = object.getClass();
        saveNewType(clazz);
        Field[] fields = clazz.getDeclaredFields();
        Path filePath = Paths.get(mainDirectory + "\\" + clazz.getSimpleName() + extension);
        Path keyFilePath = Paths.get(mainDirectory + "\\" + clazz.getSimpleName() + "Keys" + extension);

        offset = writeFieldsToFile(fields, object, filePath);
        writeKeyToFile(key, offset, keyFilePath);
    }

    private void saveNewType(Class<?> clazz) {
        if (savedTypes.containsKey(clazz.getSimpleName()))
            return;

        savedTypes.put(clazz.getSimpleName(), clazz.getName());
        File typesFile = new File(mainDirectory + "\\" + "Types" + extension);
        try (RandomAccessFile writer = new RandomAccessFile(typesFile, "rw")) {
            writer.seek(writer.length());
            byte[] buffer;
            for (String type : savedTypes.keySet()) {
                buffer = type.getBytes();
                writer.writeInt(buffer.length);
                writer.write(buffer);
                buffer = savedTypes.get(type).getBytes();
                writer.writeInt(buffer.length);
                writer.write(buffer);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readTypesFromFile() {
        File typesFile = new File(mainDirectory + "\\" + "Types" + extension);
        int length;
        String type;
        String clazz;
        try (RandomAccessFile reader = new RandomAccessFile(typesFile, "rw")) {
            reader.seek(0);
            byte[] buffer;
            while (true) {
                try {
                    length = reader.readInt();
                    buffer = new byte[length];
                    reader.read(buffer);
                    type = new String(buffer);
                    length = reader.readInt();
                    buffer = new byte[length];
                    reader.read(buffer);
                    clazz = new String(buffer);
                    savedTypes.put(type, clazz);
                } catch (EOFException e) {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void add(int key, Object object, String filePath) {
        long offset;
        Class<?> clazz = object.getClass();
        Field[] fields = clazz.getDeclaredFields();
        Path keyFilePath = Paths.get(mainDirectory + "\\" + clazz.getSimpleName() + "Keys" + extension);

        offset = writeFieldsToFile(fields, object, Paths.get(filePath));
        writeKeyToFile(key, offset, keyFilePath);
    }

    /**
     * сохраняет значения ключа и смещения в памяти
     * записывает ключ и смещение в файле с полями объекта по этому ключу, с которого началась запись этих полей
     * @param key значение ключа
     * @param offset смещение в файле, с которого были записаны поля
     * @param keyFilePath путь к файлу, в котором будут храниться значения ключа и смещения
     */
    private void writeKeyToFile(int key, long offset, Path keyFilePath) {
        writeKeyToMemory(key, offset, keyFilePath);

        try (RandomAccessFile writer = new RandomAccessFile(keyFilePath.toFile(), "rw")) {
            writer.seek(writer.length());
            writer.writeInt(key);
            writer.writeLong(offset);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * сохраняет значения ключа и смещения в файле с полями в мапу keys,
     * которая хранит имя файла, в котором находятся значения, в качестве ключа для получения этих значений
     * @param key ключ, который сохраняется в файл
     * @param offset смещение по этому ключу
     * @param keyFilePath имя файла, в котором будут храниться значения ключа и смещения
     */
    private void writeKeyToMemory(int key, long offset, Path keyFilePath) {
        HashMap<Integer, Long> keyOffsetMap = keys.get(keyFilePath);
        if (keyOffsetMap == null) {
            keyOffsetMap = new HashMap<>();
            keyOffsetMap.put(key, offset);
            keys.put(keyFilePath, keyOffsetMap);
            return;
        }
        keyOffsetMap.put(key,offset);
    }

    /**
     * считает смещение в файле, с которого начнется запись полей объекта
     * просматривает поля объекта и, если поле не помечено аннотацией @Exclude, сохраняет его в файл
     * @param fields список полей объекта, которые надо сохранить
     * @param object объект, поля которого сохраняются
     * @param filePath путь к файлу, в который будут записаны поля
     * @return возвращает смещение в файле, с которого началась запись
     */
    private long writeFieldsToFile(Field[] fields, Object object, Path filePath) {
        File file = new File(filePath.toString());
        long offset = file.length();

        for (Field field : fields) {
            field.setAccessible(true);
            Annotation annotation = field.getAnnotation(Exclude.class);
            if (annotation != null)
                continue;
            try {
                writeOneFieldToFile(field.getType(), field.get(object), filePath);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        return offset;
    }

    /**
     * сохраняет значение поля в файл
     * @param type тип записываемого поля
     * @param fieldValue значение записываемого поля
     * @param filePath путь к файлу, в который производится запись
     */
    private void writeOneFieldToFile(Class<?> type, Object fieldValue, Path filePath) {
        try (RandomAccessFile writer = new RandomAccessFile(filePath.toFile(), "rw")) {
            writer.seek(writer.length());

            switch (type.toString()) {
                case "boolean":
                    writer.writeBoolean((boolean) fieldValue);
                    break;
                case "int":
                    writer.writeInt((int) fieldValue);
                    break;
                case "long":
                    writer.writeLong((long) fieldValue);
                    break;
                case "float":
                    writer.writeFloat((float) fieldValue);
                    break;
                case "double":
                    writer.writeDouble((double) fieldValue);
                    break;
                case "class java.lang.String":
                    byte[] buffer = ((String) fieldValue).getBytes();
                    writer.writeInt(buffer.length);
                    writer.write(buffer);
                    break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * читает поля объекта из файла, строит объект нужного типа 'Т' и возвращает его пользователю
     * @param key ключ объекта, по которому производится поиск значений полей объекта в файле
     * @param type тип объекта, который нужно вернуть
     * @param <T>
     * @return возвращает объект типа 'T' с заполненныйми полями или null, если нет объекта с таким ключем
     */
    public <T> T get(int key, Class<T> type) {
        Object object = null;
        Path filePath = Paths.get(mainDirectory + "\\" + type.getSimpleName() + extension);
        Path keyFilePath = Paths.get(mainDirectory + "\\" + type.getSimpleName() + "Keys" + extension);

        long offset = getOffset(key, keyFilePath);
        if (offset < 0)
            return null;

        Field[] fields = type.getDeclaredFields();
        try {
            object = type.newInstance();
            readAllFieldsFromFile(fields, object, filePath, offset);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return (T) object;
    }

    /**
     * находит смещение в мапе по ключу
     * @param key ключ для которого надо найти смещение
     * @param keyFilePath файл, в котором записаны данные (используется как ключ для мапы)
     * @return возвращает значение смещения для заланного ключа или -1, если такого ключа нет в мапе
     */
    private long getOffset(int key, Path keyFilePath) {
        HashMap<Integer, Long> keyOffsetMap = keys.get(keyFilePath);
        Long offset = keyOffsetMap.get(key);

        return offset == null ? -1 : offset.longValue();
    }

    private void readAllFieldsFromFile(Field[] fields, Object object, Path filePath, long offset) throws IOException, IllegalAccessException {
        try (RandomAccessFile reader = new RandomAccessFile(filePath.toFile(), "rw")) {
            reader.seek(offset);
            for (Field field : fields) {
                field.setAccessible(true);
                Annotation annotation = field.getAnnotation(Exclude.class);
                if (annotation != null)
                    continue;
                field.set(object, readOneFieldFromFile(field.getType(), reader));
            }
        }
    }

    /**
     * читает значение поля из файла
     * @param type тип поля, для которого читается значение
     * @param reader объект типа RandomAccessFile для чтения полей из файла
     * @param <T> определяет какого типа данные нужно вернуть пользователю
     * @return возвращает значение поля приведенного к нужному типу 'T'
     */
    private <T> T readOneFieldFromFile(Class<T> type, RandomAccessFile reader) {
        Object fieldValue = null;
        try {
            switch (type.toString()) {
                case "boolean":
                    fieldValue = reader.readBoolean();
                    break;
                case "int":
                    fieldValue = reader.readInt();
                    break;
                case "long":
                    fieldValue = reader.readLong();
                    break;
                case "float":
                    fieldValue = reader.readFloat();
                    break;
                case "double":
                    fieldValue = reader.readDouble();
                    break;
                case "class java.lang.String":
                    int length = reader.readInt();
                    byte[] buffer = new byte[length];
                    reader.read(buffer);
                    fieldValue = new String(buffer);
                    break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (T) fieldValue;
    }

    /**
     * удаляет ключ со смещением из мапы, для того чтобы нельзя было прочитать значения полей с этим ключом
     * помечает в каких файлах было произведено удаление, чтобы их можно было переписать
     * @param key удаляемый ключ
     * @param type тип удаляемого объекта (определяет в каком файле находятся поля этогго объекта)
     */
    public void remove(int key, Class<?> type) {
        Path filePath = Paths.get(mainDirectory + "\\" + type.getSimpleName() + extension);
        Path keyFilePath = Paths.get(mainDirectory + "\\" + type.getSimpleName() + "Keys" + extension);
        removeKeyFromMemory(key, keyFilePath);

        updateFilesToRewrite(filePath);
    }

    private void removeKeyFromMemory(int key, Path keyFilePath) {
        HashMap<Integer, Long> keyOffsetMap = keys.get(keyFilePath);
        keyOffsetMap.remove(key);
    }

    /**
     * помечает какие файлы нужно переписать из-за внесенных в них изменений (remove/update)
     * @param file файл который нужно пометить для перехаписи
     */
    private void updateFilesToRewrite(Path file) {
        rewriteStatus.put(file, true);
    }

    public void update(int key, Object object) {
        long offset;
        Class<?> clazz = object.getClass();
        Field[] fields = clazz.getDeclaredFields();
        Path filePath = Paths.get(mainDirectory + "\\" + clazz.getSimpleName() + extension);
        Path keyFilePath = Paths.get(mainDirectory + "\\" + clazz.getSimpleName() + "Keys" + extension);

        offset = writeFieldsToFile(fields, object, filePath);
        writeKeyToMemory(key, offset, keyFilePath);
        updateFilesToRewrite(filePath);
    }

    /**
     * переписывает каждый файл, который был помечен как измененный
     */
    public void rewriteFiles() {
        for (Map.Entry<Path, Boolean> entry : rewriteStatus.entrySet()) {
            if (entry.getValue()) {
                try {
                    rewriteOneFile(entry.getKey());
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * переписывает один из списка файлов: сначала читает поля из старого файла по ключу из памяти,
     * затем записывает их в новый файл и обновляет ключ в памяти
     * @param filePath
     */
    private void rewriteOneFile(Path filePath) throws ClassNotFoundException {
        String fileName = filePath.getFileName().toString();
        String tmpFile = mainDirectory + "\\" + fileName.substring(0, fileName.length() - 5) + ".tmp";
        Class<?> type = Class.forName(savedTypes.get(fileName.substring(0, fileName.length() - 5)));
        Object object;

        Path keyFilePath = Paths.get(mainDirectory + "\\" + type.getSimpleName() + "Keys" + extension);
        HashMap<Integer, Long> keyOffsetMap = keys.get(keyFilePath);
        new File(keyFilePath.toString()).delete();

        for (Integer key : keyOffsetMap.keySet()) {
            object = get(key, type);
            add(key, type.cast(object), tmpFile);
        }

        new File(filePath.toString()).delete();
        new File(tmpFile).renameTo(filePath.toFile());
    }
}
