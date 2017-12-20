package com.itmo;

import java.io.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class KVDataBase {

    // разные типы объектов сохраняются в разные таблицы, которые являются разными файлами да диске

    // списки объектов для обработки (сохранение)
    Map<Integer, Object> itemsToAdd;

    public KVDataBase() {
        itemsToAdd = new HashMap<>();
    }

    // помещаем объект в коллекцию, которая в последствии будет записана на диск
    public void add(int key, Object object) {
        if (object != null)
            itemsToAdd.put(key, object);
    }

    // todo поиск объекта в файле на диске
    public Object get(Object object) {
        return null;
    }

    /* функция для фоновой записи объектов в файл на диске или их удаления с диска*/
    public void save() {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    writeObjectsToFile();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        });

        thread.start();
    }

    private void writeObjectsToFile() throws IOException, IllegalAccessException {
        String fileName;
        for (Map.Entry<Integer, Object> entry : itemsToAdd.entrySet()) {
            Class objClass = entry.getValue().getClass();
            fileName = objClass.getSimpleName() + ".kvdb"; // название таблицы будет такое же как и имя класса, объекты которого мы будем хранить в ней
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
        itemsToAdd.clear();
    }

    private void writeKeyToFile(int key, String fileName) throws IOException {
        String indexFileName = "Indices.kvdb";
        DataOutputStream out;
        File file = new File(fileName);
        long offset = 0;

        if (file.exists() && file.isFile()) {
            offset = file.length();
        }

        // сохранение ключа в основном файле
        out = new DataOutputStream(new FileOutputStream(fileName, true));
        out.writeInt(key);
        out.close();

        // сохранение смещения ключа
        // как потом искать, в каком файле?
        out = new DataOutputStream(new FileOutputStream(indexFileName, true));
        out.writeInt(key);
        out.writeLong(offset);
        out.close();
    }

    private void writeFieldToFile(Class type, Object data, String fileName) throws IOException {
        try(DataOutputStream out = new DataOutputStream(new FileOutputStream(fileName, true))) {
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
