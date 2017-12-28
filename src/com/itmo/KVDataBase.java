package com.itmo;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;

public class KVDataBase {
    private String mainDirectory;
    private final String extension = ".kvdb";
    private HashMap<Path, HashMap<Integer, Long>> keys;
    private HashMap<Path, Boolean> rewriteStatus;
    private HashMap<String, String> savedTypes;
    private HashMap<Path, RandomAccessFile> filesInDirectory;

    private final Map<Class<?>, DbDescriptor> descriptors = new HashMap<>();

    private KVDataBase() {
        keys = new HashMap<>();
        rewriteStatus = new HashMap<>();
        savedTypes = new HashMap<>();
        filesInDirectory = new HashMap<>();
    }

    /**
     * создает объект KVDataBase, передает ему путь к директории, где лежат файлы с данными (указывается пользователем).
     * если такой директории не существует, создает ее
     * читает key-файлы в память, если они присутствуют в директории
     *
     * @param directoryPath путь к директории с файлами
     * @return возвращает новый обект KVDataBase
     */
    public static KVDataBase open(String directoryPath) {
        System.out.println("Database opening, please stand by...");
        KVDataBase dataBase = new KVDataBase();
        dataBase.mainDirectory = directoryPath;
        File directory = new File(dataBase.mainDirectory);
        if (!directory.exists())
            directory.mkdir();

        try {
            dataBase.openAllFiles();
            dataBase.readKeyFiles();
            dataBase.readTypesFromFile();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        return dataBase;
    }

    private void openAllFiles() throws IOException {
        RandomAccessFile raf;
        try (DirectoryStream<Path> directory = Files.newDirectoryStream(Paths.get(mainDirectory))) {
            for (Path file : directory) {
                raf = new RandomAccessFile(file.toFile(), "rw");
                filesInDirectory.put(file, raf);
            }
        }
    }

    public void close() {
        try {
            for (RandomAccessFile file : filesInDirectory.values()) {
                file.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * просматривает имена файлов и сохраняет их в мапу со статусом перезаписи (true/false)
     * читает данные из файлов с ключами, сохраняет их в мапу 'путь - мапа ключей'
     */
    private void readKeyFiles() throws IOException {
        HashMap<Integer, Long> keyOffsetMap;

        for (Map.Entry<Path, RandomAccessFile> entry : filesInDirectory.entrySet()) {
            if (entry.getKey().toString().contains("Keys")) {
                keyOffsetMap = readKeysAndOffsetsFromFile(entry.getValue());
                keys.put(entry.getKey(), keyOffsetMap);
            }
        }
    }

    /**
     * читает значения ключей и смещений из файла
     * @param reader читает значения из файла с ключами
     * @return возвращает мапу 'ключ - смещение'
     * @throws IOException
     */
    private HashMap<Integer, Long> readKeysAndOffsetsFromFile(RandomAccessFile reader) throws IOException {
        HashMap<Integer, Long> keyOffsetMap = new HashMap<>();
        byte[] buffer = new byte[Integer.BYTES + Long.BYTES];
        byte[] valueInBytes;
        int key;
        long offset;

        while (reader.read(buffer) > 0) {
            System.arraycopy(buffer, 0, valueInBytes = new byte[Integer.BYTES], 0, Integer.BYTES);
            key = ByteBuffer.wrap(valueInBytes).getInt();
            System.arraycopy(buffer, Integer.BYTES, valueInBytes = new byte[Long.BYTES], 0, Long.BYTES);
            offset = ByteBuffer.wrap(valueInBytes).getLong();
            keyOffsetMap.put(key, offset);
        }

        return keyOffsetMap;
    }

    /**
     * добавляет объект в базу данных: сохраняет значение его полей в файл
     * сохраняет его смещение по ключу в файле с ключами
     *
     * @param key    ключ для дальнейшего поиска объекта
     * @param object объект, который надо добавить в базу данных
     */
    public void add(int key, Object object) {
        long offset;
        Class<?> clazz = object.getClass();

        try {
            saveNewType(clazz);
            DbDescriptor descriptor = getDbDescriptor(clazz);
            offset = writeFieldsToFile(object, descriptor);
            writeKeyToFile(key, offset, descriptor);

        } catch (IOException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private DbDescriptor getDbDescriptor(Class<?> clazz) throws FileNotFoundException {
        if (!descriptors.containsKey(clazz)) {
            Field[] fields = clazz.getDeclaredFields();
            Path filePath = Paths.get(mainDirectory + "\\" + clazz.getSimpleName() + extension);
            Path keyFilePath = Paths.get(mainDirectory + "\\" + clazz.getSimpleName() + "Keys" + extension);

            RandomAccessFile writer = filesInDirectory.get(filePath);
            if (writer == null) {
                writer = new RandomAccessFile(filePath.toFile(), "rw");
                filesInDirectory.put(filePath, writer);
            }
            RandomAccessFile keyWriter = filesInDirectory.get(keyFilePath);
            if (keyWriter == null) {
                keyWriter = new RandomAccessFile(keyFilePath.toFile(), "rw");
                filesInDirectory.put(keyFilePath, keyWriter);
            }

            List<Field> flds = new ArrayList<>();

            for (Field fld : fields) {
                if (fld.getAnnotation(Exclude.class) == null) {
                    fld.setAccessible(true);

                    flds.add(fld);
                }
            }

            descriptors.put(clazz,
                    new DbDescriptor(filePath, writer, keyFilePath, keyWriter, flds.toArray(new Field[flds.size()])));
        }

        return descriptors.get(clazz);
    }


    /**
     * сохраняет в мапу полные имена типов объектов, которые сохраняются в базе
     *
     * @param newType тип, полное имя которого надо сохранить
     */
    private void saveNewType(Class<?> newType) throws IOException {
        if (savedTypes.containsKey(newType.getSimpleName()))
            return;

        savedTypes.put(newType.getSimpleName(), newType.getName());
        writeNewTypeToFile(newType.getSimpleName(), newType.getName());
    }

    /**
     * сохраняет полное имя типа объекта, который был сохранен в базе, в файл
     *
     * @param typeSimpleName короткое имя типа (класса)
     * @param typeFullName   полное имя типа
     */
    private void writeNewTypeToFile(String typeSimpleName, String typeFullName) throws IOException {
        File typesFile = new File(mainDirectory + "\\" + "Types" + extension);
        RandomAccessFile writer = filesInDirectory.get(typesFile.toPath());
        if (writer == null) {
            writer = new RandomAccessFile(typesFile, "rw");
            filesInDirectory.put(typesFile.toPath(), writer);
        }

        writer.seek(writer.length());
        byte[] buffer = typeSimpleName.getBytes();
        writer.writeInt(buffer.length);
        writer.write(buffer);
        buffer = typeFullName.getBytes();
        writer.writeInt(buffer.length);
        writer.write(buffer);
    }

    /**
     * читает из файла в память список всех полных имен хранимых в базе типов объектов
     */
    private void readTypesFromFile() throws IOException, ClassNotFoundException {
        File typesFile = new File(mainDirectory + "\\" + "Types" + extension);
        if (!typesFile.exists())
            return;

        int length;
        String type;
        String clazz;
        RandomAccessFile reader = filesInDirectory.get(typesFile.toPath());

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
                getDbDescriptor(Class.forName(clazz));
            } catch (EOFException e) {
                break;
            }
        }
    }

    /**
     * служебный метод используется для перезаписи файлов после внесения в них изменений
     *
     * @param key      ключ объекта, который надо перезаписать
     * @param object   объект, который надо перезаписать
     * @param filePath имя временного файла в который будет записан объект (в дальнейшем переименовывается в рабочий файл)
     */
    private void add(int key, Object object, String filePath) {
        long offset;
        Class<?> clazz = object.getClass();

        try {
            DbDescriptor descriptor = getDbDescriptor(clazz);

            offset = writeFieldsToFile(object, descriptor);
            writeKeyToFile(key, offset, descriptor);
        } catch (IllegalAccessException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * сохраняет значения ключа и смещения в памяти
     * записывает ключ и смещение в файле с полями объекта по этому ключу, с которого началась запись этих полей
     * @param key значение ключа, которое надо сохранить в файл
     * @param offset смещение по этому ключу
     * @param descriptor дескриптор сохраняемого объекта, содержит пути к файлам с данными и ключами
     *             и объекты для работы с этими файлами (запись/чтение)
     * @throws IOException
     */
    private void writeKeyToFile(int key, long offset, DbDescriptor descriptor) throws IOException {
        writeKeyToMemory(key, offset, descriptor.keyFilePath);

        byte[] buffer = new byte[Integer.BYTES + Long.BYTES];
        byte[] keyBytes = ByteBuffer.allocate(Integer.BYTES).putInt(key).array();
        byte[] offsetBytes = ByteBuffer.allocate(Long.BYTES).putLong(offset).array();
        System.arraycopy(keyBytes, 0, buffer, 0, keyBytes.length);
        System.arraycopy(offsetBytes, 0, buffer, keyBytes.length, offsetBytes.length);
        RandomAccessFile writer = descriptor.keyFile;
        writer.seek(writer.length());
        writer.write(buffer);
    }

    /**
     * сохраняет значения ключа и смещения в файле с полями в мапу keys,
     * которая хранит имя файла, в котором находятся значения, в качестве ключа для получения этих значений
     *
     * @param key         ключ, который сохраняется в файл
     * @param offset      смещение по этому ключу
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
        keyOffsetMap.put(key, offset);
    }

    /**
     * считает смещение в файле, с которого начнется запись полей объекта
     * просматривает поля объекта и, если поле не помечено аннотацией @Exclude, сохраняет его в файл
     * @param object объект, который нужно сохранить в файл
     * @param descriptor дескриптор сохраняемого объекта, содержит пути к файлам с данными и ключами
     *             и объекты для работы с этими файлами (запись/чтение)
     * @return возвращает смещение, с которого началась запись в файл
     * @throws IllegalAccessException
     * @throws IOException
     */
    private long writeFieldsToFile(Object object, DbDescriptor descriptor) throws IllegalAccessException, IOException {
        File file = new File(descriptor.filePath.toString());
        long offset = file.length();

        List<byte[]> fieldsInBytes = new ArrayList();
        byte[] buffer;
        int capacity = 0;
        int position = 0;

        for (Field field : descriptor.fields) {
            buffer = convertFieldToBytes(field.getType(), field.get(object));
            capacity += buffer.length;
            fieldsInBytes.add(buffer);
        }

        buffer = new byte[capacity];
        for (byte[] field : fieldsInBytes) {
            System.arraycopy(field, 0, buffer, position, field.length);
            position += field.length;
        }

        RandomAccessFile writer = descriptor.dbFile;
        writer.seek(writer.length());
        writer.write(buffer);

        return offset;
    }

    /**
     * преобразует значение поля в массив байт
     * @param type тип поля для преобразования
     * @param fieldValue значение преобразуемого поля
     * @return возвращает массив байт полученный при преобразовании
     */
    private byte[] convertFieldToBytes(Class<?> type, Object fieldValue) {
        byte[] buffer = null;

        switch (type.toString()) {
            case "boolean":
                buffer = ByteBuffer.allocate(Byte.BYTES).put((byte) ((boolean) fieldValue ? 1 : 0)).array();
                break;
            case "int":
                buffer = ByteBuffer.allocate(Integer.BYTES).putInt((int) fieldValue).array();
                break;
            case "long":
                buffer = ByteBuffer.allocate(Long.BYTES).putLong((long) fieldValue).array();
                break;
            case "float":
                buffer = ByteBuffer.allocate(Float.BYTES).putFloat((float) fieldValue).array();
                break;
            case "double":
                buffer = ByteBuffer.allocate(Double.BYTES).putDouble((double) fieldValue).array();
                break;
            case "class java.lang.String":
                byte[] strBytes = ((String) fieldValue).getBytes();
                byte[] strLen = ByteBuffer.allocate(Integer.BYTES).putInt(strBytes.length).array();
                buffer = new byte[strBytes.length + strLen.length];
                System.arraycopy(strLen, 0, buffer, 0, strLen.length);
                System.arraycopy(strBytes, 0, buffer, strLen.length, strBytes.length);
                break;
        }
        return buffer;
    }

    /**
     * читает поля объекта из файла, строит объект нужного типа 'Т' и возвращает его пользователю
     *
     * @param key  ключ объекта, по которому производится поиск значений полей объекта в файле
     * @param type тип объекта, который нужно вернуть
     * @param <T>
     * @return возвращает объект типа 'T' с заполненныйми полями или null, если нет объекта с таким ключем
     */
    public <T> T get(int key, Class<T> type) {
        Object object = null;

        try {
            DbDescriptor descriptor = getDbDescriptor(type);

            long offset = getOffset(key, descriptor.keyFilePath);
            if (offset < 0)
                return null;

            object = type.newInstance();
            readAllFieldsFromFile(object, offset, descriptor);
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return (T) object;
    }

    /**
     * возвращает объект, который соответствует некоторому критерию
     *
     * @param <T>
     * @return
     */
    public <T> T find() {
        try {
            throw new Exception();
        } catch (Exception e) {
            System.out.println("Not implemented method.");
        }
        return null;
    }

    /**
     * находит смещение в мапе по ключу
     *
     * @param key         ключ для которого надо найти смещение
     * @param keyFilePath файл, в котором записаны данные (используется как ключ для мапы)
     * @return возвращает значение смещения для заланного ключа или -1, если такого ключа нет в мапе
     */
    private long getOffset(int key, Path keyFilePath) {
        HashMap<Integer, Long> keyOffsetMap = keys.get(keyFilePath);
        Long offset = keyOffsetMap.get(key);

        return offset == null ? -1 : offset.longValue();
    }

    /**
     * читает значения полей из файла и устанавливает их переданным полям объекта
     * @param object объект, для которого читаются поля
     * @param offset смещение в файле, с которого начинаются нужные данные
     * @param descriptor дескриптор сохраняемого объекта, содержит пути к файлам с данными и ключами
     *             и объекты для работы с этими файлами (запись/чтение)
     * @throws IOException
     * @throws IllegalAccessException
     */
    private void readAllFieldsFromFile(Object object, long offset, DbDescriptor descriptor) throws IOException, IllegalAccessException {
        RandomAccessFile reader = descriptor.dbFile;
        reader.seek(offset);

        for (Field field : descriptor.fields) {
            field.set(object, readOneFieldFromFile(field.getType(), reader));
        }
    }

    /**
     * читает значение поля из файла
     *
     * @param type   тип поля, для которого читается значение
     * @param reader объект типа RandomAccessFile для чтения полей из файла
     * @param <T>    определяет какого типа данные нужно вернуть пользователю
     * @return возвращает значение поля приведенного к нужному типу 'T'
     */
    private <T> T readOneFieldFromFile(Class<T> type, RandomAccessFile reader) throws IOException {
        Object fieldValue = null;

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

        return (T) fieldValue;
    }

    /**
     * удаляет ключ со смещением из мапы, для того чтобы нельзя было прочитать значения полей с этим ключом
     * помечает в каких файлах было произведено удаление, чтобы их можно было переписать
     *
     * @param key  удаляемый ключ
     * @param type тип удаляемого объекта (определяет в каком файле находятся поля этогго объекта)
     */
    public void remove(int key, Class<?> type) {
        Path filePath = Paths.get(mainDirectory + "\\" + type.getSimpleName() + extension);
        Path keyFilePath = Paths.get(mainDirectory + "\\" + type.getSimpleName() + "Keys" + extension);
        removeKeyFromMemory(key, keyFilePath);

        updateFilesToRewrite(filePath);
    }

    /**
     * удаляет ключ и смещение, хранящиеся в мапе при удалении объекта из базы
     *
     * @param key         ключ объекта, который удаляется
     * @param keyFilePath имя файла, в котором хранится ключ удаляемого объекта
     */
    private void removeKeyFromMemory(int key, Path keyFilePath) {
        HashMap<Integer, Long> keyOffsetMap = keys.get(keyFilePath);
        keyOffsetMap.remove(key);
    }

    /**
     * помечает какие файлы нужно переписать из-за внесенных в них изменений (remove/update)
     *
     * @param file файл который нужно пометить для перехаписи
     */
    private void updateFilesToRewrite(Path file) {
        rewriteStatus.put(file, true);
    }

    /**
     * обновить в файле значение полей объекта с данным ключом
     *
     * @param key    ключ объекта, кданные которого надо обновить
     * @param object новый объект, значения полей которого заменят старые значения в файле.
     */
    public void update(int key, Object object) {
        long offset;
        try {
            DbDescriptor descriptor = getDbDescriptor(object.getClass());
            offset = writeFieldsToFile(object, descriptor);
            writeKeyToMemory(key, offset, descriptor.keyFilePath);
            updateFilesToRewrite(descriptor.filePath);
        } catch (FileNotFoundException | IllegalAccessException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
     *
     * @param filePath
     */
    private void rewriteOneFile(Path filePath) throws ClassNotFoundException {
//        String fileName = filePath.getFileName().toString();
//        String tmpFile = mainDirectory + "\\" + fileName.substring(0, fileName.length() - 5) + ".tmp";
//        Class<?> type = Class.forName(savedTypes.get(fileName.substring(0, fileName.length() - 5)));
//        Object object;
//
//        Path keyFilePath = Paths.get(mainDirectory + "\\" + type.getSimpleName() + "Keys" + extension);
//        HashMap<Integer, Long> keyOffsetMap = keys.get(keyFilePath);
//        new File(keyFilePath.toString()).delete();
//
//        for (Integer key : keyOffsetMap.keySet()) {
//            object = get(key, type);
//            add(key, type.cast(object), tmpFile);
//        }
//
//        new File(filePath.toString()).delete();
//        new File(tmpFile).renameTo(filePath.toFile());
    }
}
