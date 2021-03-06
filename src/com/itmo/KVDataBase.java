package com.itmo;

import com.itmo.exceptions.EndOfFileException;
import com.itmo.exceptions.KeyNotFoundException;
import com.itmo.exceptions.ObjectDataNotFound;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

public class KVDataBase {
    private static Map<Class<?>, List<Object>> cache = new HashMap<>();
    private static final Map<String, KVDataBase> instances = new HashMap<>();
    private static final Map<String, Integer> instancesCount = new HashMap<>();
    private String mainDirectory;
    private final String extension = ".kvdb";
    private HashMap<Path, HashMap<Integer, Long>> keys;
    private HashMap<Path, RandomAccessFile> filesInDirectory;
    private ReadWriteLock lock;
    private Semaphore semaphore;

    private final Map<Class<?>, DbDescriptor> descriptors = new HashMap<>();

    private KVDataBase() {
        keys = new HashMap<>();
        filesInDirectory = new HashMap<>();
        lock = new ReentrantReadWriteLock();
        semaphore = new Semaphore(10);
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
        KVDataBase dataBase = KVDataBase.instances.get(directoryPath);
        if (dataBase == null) {
            System.out.println("Database opening, please stand by...");
            dataBase = new KVDataBase();
            dataBase.mainDirectory = directoryPath;
            File directory = new File(dataBase.mainDirectory);
            if (!directory.exists())
                directory.mkdir();

            try {
                dataBase.openAllFiles();
                dataBase.readKeyFiles();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            System.out.println("Database ready.");
            instances.put(directoryPath, dataBase);
        }
        if (!instancesCount.containsKey(directoryPath)) {
            instancesCount.put(directoryPath, 1);
        } else {
            instancesCount.put(directoryPath, instancesCount.get(directoryPath) + 1);
        }
        return dataBase;
    }

    /**
     * открывает все файлы, находящиеся в директории на запись/чтение
     *
     * @throws IOException
     */
    private void openAllFiles() throws IOException {
        RandomAccessFile raf;
        try (DirectoryStream<Path> directory = Files.newDirectoryStream(Paths.get(mainDirectory))) {
            for (Path file : directory) {
                raf = new RandomAccessFile(file.toFile(), "rw");
                filesInDirectory.put(file, raf);
            }
        }
    }

    public static void close(String directoryPath) {
        if (instancesCount.get(directoryPath) > 1) {
            instancesCount.put(directoryPath, instancesCount.get(directoryPath) - 1);
            return;
        }

        KVDataBase db = instances.get(directoryPath);
        if (db == null) {
            return;
        }

        try {
            // закрыть все ридеры
            for (DbDescriptor descriptor : db.descriptors.values()) {
                for (int i = 0; i < descriptor.readers.length; i++) {
                    descriptor.readers[i].getReader().close();
                }
            }

            for (RandomAccessFile file : db.filesInDirectory.values()) {
                file.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        instances.remove(db.mainDirectory);
        instancesCount.remove(db.mainDirectory);
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
     *
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
        Class<?> type = object.getClass();
        try {
            DbDescriptor descriptor = getDbDescriptor(type);
            doUpdate(descriptor, key, object);
        } catch (IOException | IllegalAccessException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void doUpdate(DbDescriptor descriptor, int key, Object object) throws IOException, IllegalAccessException, InterruptedException {
        long offset;
        try {
            lock.writeLock().lock();
            offset = writeObjectToFile(object, descriptor);
            writeKeyToFile(key, offset, descriptor);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * возвращает дескриптор класса объекта
     * открывает файлы, нужные для сохранения или чтения данных для объектов данного класса
     *
     * @param clazz класс объекта, для которого нужно получить дескриптор
     * @return
     * @throws FileNotFoundException
     */
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
     * сохраняет значения ключа и смещения в памяти
     * записывает ключ и смещение в файле с полями объекта по этому ключу, с которого началась запись этих полей
     *
     * @param key        значение ключа, которое надо сохранить в файл
     * @param offset     смещение по этому ключу
     * @param descriptor дескриптор сохраняемого объекта, содержит пути к файлам с данными и ключами
     *                   и объекты для работы с этими файлами (запись/чтение)
     * @throws IOException
     */
    private void writeKeyToFile(int key, long offset, DbDescriptor descriptor) throws IOException {
        writeKeyToMemory(key, offset, descriptor.keyFilePath);

        byte[] buffer = new byte[Integer.BYTES + Long.BYTES];
        byte[] keyBytes = ByteBuffer.allocate(Integer.BYTES).putInt(key).array();
        byte[] offsetBytes = ByteBuffer.allocate(Long.BYTES).putLong(offset).array();
        System.arraycopy(keyBytes, 0, buffer, 0, keyBytes.length);
        System.arraycopy(offsetBytes, 0, buffer, keyBytes.length, offsetBytes.length);
        RandomAccessFile writer = descriptor.keyFileWriter;
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
        HashMap<Integer, Long> map = keys.get(keyFilePath);
        if (map == null) {
            map = new HashMap<>();
        }
        map.put(key, offset);
        keys.put(keyFilePath, map);
    }

    /**
     * считает смещение в файле, с которого начнется запись полей объекта
     * просматривает поля объекта и, если поле не помечено аннотацией @Exclude, сохраняет его в файл
     *
     * @param object     объект, который нужно сохранить в файл
     * @param descriptor дескриптор сохраняемого объекта, содержит пути к файлам с данными и ключами
     *                   и объекты для работы с этими файлами (запись/чтение)
     * @return возвращает смещение, с которого началась запись в файл
     * @throws IllegalAccessException
     * @throws IOException
     */
    private long writeObjectToFile(Object object, DbDescriptor descriptor) throws IllegalAccessException, IOException {
        File file = new File(descriptor.filePath.toString());
        RandomAccessFile writer = descriptor.dbFileWriter;
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

        writer.seek(offset);
        writer.write(buffer);

        return offset;
    }

    /**
     * преобразует значение поля в массив байт
     *
     * @param type       тип поля для преобразования
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
    public <T> T getByKey(int key, Class<T> type) {
        Object object;
        try {
            semaphore.acquire();
            lock.readLock().lock();

            DbDescriptor descriptor = getDbDescriptor(type);

            long offset = getOffset(key, descriptor.keyFilePath);
            if (offset < 0)
                return null;

            object = readObjectByOffset(type, offset, descriptor);
        } catch (InstantiationException | IllegalAccessException | InterruptedException | IOException e) {
            throw new RuntimeException(e);
        } finally {
            lock.readLock().unlock();
            semaphore.release();
        }

        return (T) object;
    }

    /**
     * возвращает объект, который соответствует некоторому критерию
     *
     * @param <T>
     * @return
     */

    public <T> T findFirst(Class<T> type, Predicate<T> predicate) {
        Object object = null;
        DbDescriptor descriptor = null;
        Reader reader = null;

        try {
            semaphore.acquire();
            lock.readLock().lock();

            descriptor = getDbDescriptor(type);
            reader = descriptor.getReader();
            RandomAccessFile raf = reader.getReader();

            for (Long offset : keys.get(descriptor.keyFilePath).values()) {
                if (offset < 0)
                    continue;
                raf.seek(offset);
                object = readObjectFromFile(type, raf, descriptor);
                if (predicate.test((T) object))
                    break;
            }
            if (object == null)
                return (T) object;
            if (!predicate.test((T) object))
                object = null;

        } catch (IOException | IllegalAccessException | InstantiationException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            if (descriptor != null)
                descriptor.releaseReader(reader);
            lock.readLock().unlock();
            semaphore.release();
        }

        return (T) object;
    }

    private <T> T readObjectFromFile(Class<T> type, RandomAccessFile reader, DbDescriptor descriptor) throws IllegalAccessException, InstantiationException, IOException {
        Object object;
        try {
            object = type.newInstance();
            for (Field field : descriptor.fields) {
                field.set(object, readOneFieldFromFile(field.getType(), reader));
            }
        } catch (EndOfFileException e) {
            return null;
        }
        return (T) object;
    }

    private <T> List<T> searchInCache(Class<T> type, Predicate<T> predicate) {
        if (cache.size() == 0) {
            return null;
        }

        List<T> objects = (List<T>) cache.get(type);
        if (objects == null)
            return null;

        List<T> retList = new ArrayList<>();
        for (T object : objects) {
            if (predicate.test(object))
                retList.add(object);
        }

        return retList;
    }

    public <T> List<T> findAll(Class<T> type, Predicate<T> predicate) {
        List<T> list = searchInCache(type, predicate);
        if (list != null) {
            return list;
        }

        list = new ArrayList<>();
        List<Object> cacheList = new ArrayList<>();
        Object object;

        DbDescriptor descriptor = null;
        Reader reader = null;
        try {
            semaphore.acquire();
            lock.readLock().lock();

            descriptor = getDbDescriptor(type);
            reader = descriptor.getReader();
            RandomAccessFile raf = reader.getReader();

            for (Long offset : keys.get(descriptor.keyFilePath).values()) {
                if (offset < 0)
                    continue;
                raf.seek(offset);
                object = readObjectFromFile(type, raf, descriptor);
                cacheList.add(object);
                if (predicate.test((T) object))
                    list.add((T) object);
            }
            cache.put(type, cacheList);

        } catch (IOException | IllegalAccessException | InstantiationException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            if (descriptor != null)
                descriptor.releaseReader(reader);
            lock.readLock().unlock();
            semaphore.release();
        }

        return list.size() == 0 ? null : list;
    }

    /**
     * находит смещение в мапе по ключу
     *
     * @param key         ключ для которого надо найти смещение
     * @param keyFilePath файл, в котором записаны данные (используется как ключ для мапы)
     * @return возвращает значение смещения для заланного ключа или -1, если такого ключа нет в мапе
     */
    private long getOffset(int key, Path keyFilePath) {
        try {
            Long offset = keys.get(keyFilePath).get(key);
            return offset == null ? -1 : offset;
        } catch (NullPointerException e) {
            throw new KeyNotFoundException(e.getMessage());
        }
    }

    /**
     * читает значения полей из файла и устанавливает их переданным полям объекта
     *
     * @param type       тип объекта, который нужно прочитать и вернуть
     * @param offset     смещение в файле, с которого начинаются нужные данные
     * @param descriptor дескриптор сохраняемого объекта, содержит пути к файлам с данными и ключами
     *                   и объекты для работы с этими файлами (запись/чтение)
     * @throws IOException
     * @throws IllegalAccessException
     */
    private <T> T readObjectByOffset(Class<T> type, long offset, DbDescriptor descriptor) throws IOException, IllegalAccessException, InstantiationException {
        Object object = type.newInstance();
        Reader reader = descriptor.getReader();
        RandomAccessFile raf = reader.getReader();

        raf.seek(offset);

        for (Field field : descriptor.fields) {
            field.set(object, readOneFieldFromFile(field.getType(), raf));
        }

        descriptor.releaseReader(reader);
        return (T) object;
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
        } catch (EOFException e) {
            throw new EndOfFileException(e.getMessage());
        }

        return (T) fieldValue;
    }

    /**
     * удаляет ключ со смещением из мапы, переписывает значение ключа в файле,
     * чтобы, при открытии базы, объект оставался помеченным как "удаленный"
     *
     * @param key  удаляемый ключ
     * @param type тип удаляемого объекта (определяет в каком файле находятся поля этогго объекта)
     */
    public void remove(int key, Class<?> type) {
        try {
            DbDescriptor descriptor = getDbDescriptor(type);
            removeKeyFromMemory(key, descriptor.keyFilePath);
            writeKeyToFile(key, -1, descriptor);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * удаляет ключ и смещение, хранящиеся в мапе при удалении объекта из базы
     *
     * @param key         ключ объекта, который удаляется
     * @param keyFilePath имя файла, в котором хранится ключ удаляемого объекта
     */
    private void removeKeyFromMemory(int key, Path keyFilePath) {
        try {
            HashMap<Integer, Long> keyOffsetMap = keys.get(keyFilePath);
            keyOffsetMap.remove(key);
        } catch (NullPointerException e) {
            throw new KeyNotFoundException(e.getMessage());
        }
    }

    /**
     * обновить в файле значение полей объекта с данным ключом
     *
     * @param key    ключ объекта, данные которого надо обновить
     * @param object новый объект, значения полей которого заменят старые значения в файле.
     */
    public void update(int key, Object object) {
        try {
            DbDescriptor descriptor = getDbDescriptor(object.getClass());
            long offset = getOffset(key, descriptor.keyFilePath);
            if (offset < 0)
                throw new ObjectDataNotFound();

            Object obj = readObjectByOffset(object.getClass(), offset, descriptor);
            if (obj == null)
                throw new ObjectDataNotFound();

            add(key, object);
        } catch (IllegalAccessException | InstantiationException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * переписывает указанный файл с данныйми
     *
     * @param type определяет таблицу для которой нужно переписать файл (имя файла)
     */
    public void truncate(Class<?> type) {
        lock.writeLock().lock();
        File file = new File(mainDirectory + File.separator + type.getSimpleName() + extension);
        if (!file.exists())
            return;

        try {
            DbDescriptor descriptor = getDbDescriptor(type);
            File newDataFile = new File(descriptor.filePath.toString() + ".tmp");
            File newKeyFile = new File(descriptor.keyFilePath.toString() + ".tmp");
            rewriteFile(type, descriptor, newDataFile, newKeyFile);
            // закрытие всех RAF
            for (int i = 0; i < descriptor.readers.length; i++) {
                descriptor.readers[i].getReader().close();
            }
            descriptor.dbFileWriter.close();
            descriptor.keyFileWriter.close();
            filesInDirectory.get(descriptor.filePath).close();
            filesInDirectory.get(descriptor.keyFilePath).close();
            filesInDirectory.remove(descriptor.filePath);
            filesInDirectory.remove(descriptor.keyFilePath);
            // переименование файлов, удаление старых
            descriptor.filePath.toFile().delete();
            descriptor.keyFilePath.toFile().delete();
            newDataFile.renameTo(descriptor.filePath.toFile());
            newKeyFile.renameTo(descriptor.keyFilePath.toFile());
            descriptors.remove(type);
            // открытие новых RAF
            getDbDescriptor(type);
        } catch (IOException | IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Переписывает указанный файл, оставляя только актуальные значения, а так же файл его ключей
     *
     * @param type        определяет для какой таблицы будет переписан файл (имя файла)
     * @param descriptor  дескриптор сохраняемого объекта, содержит пути к файлам с данными и ключами
     *                    и объекты для работы с этими файлами (запись/чтение)
     * @param newDataFile путь к новому файлу с данными
     * @param newKeyFile  путь к новому файлу с ключами
     * @throws IOException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private void rewriteFile(Class<?> type, DbDescriptor descriptor, File newDataFile, File newKeyFile) throws IOException, InstantiationException, IllegalAccessException {
        Object object;
        Long offset;

        try (RandomAccessFile dataWriter = new RandomAccessFile(newDataFile, "rw");
             RandomAccessFile keyWriter = new RandomAccessFile(newKeyFile, "rw")) {
            DbDescriptor tmpDesc = new DbDescriptor(newDataFile.toPath(), dataWriter, newKeyFile.toPath(), keyWriter, descriptor.fields);
            for (Map.Entry<Integer, Long> entry : keys.get(descriptor.keyFilePath).entrySet()) {
                if (entry.getValue() < 0)
                    continue;
                object = readObjectByOffset(type, entry.getValue(), descriptor);
                offset = writeObjectToFile(object, tmpDesc);
                writeKeyToFile(entry.getKey(), offset, tmpDesc);
            }
            for (int i = 0; i < tmpDesc.readers.length; i++) {
                tmpDesc.readers[i].getReader().close();
            }
            tmpDesc.dbFileWriter.close();
            tmpDesc.keyFileWriter.close();
        }
    }
}
