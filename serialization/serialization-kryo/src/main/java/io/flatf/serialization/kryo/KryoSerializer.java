package io.flatf.serialization.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public final class KryoSerializer implements AutoCloseable {

    private final ThreadLocal<Kryo> threadLocalKryo;

    public KryoSerializer(Class<?>... classes) {
        threadLocalKryo = ThreadLocal.withInitial(() -> {
            Kryo kryo = new Kryo();
            kryo.setReferences(true);  // 支持对象循环引用
            for (Class<?> clazz : classes) {
                kryo.register(clazz); // 注册类
            }
            return kryo;
        });
    }

    public byte[] serialize(Object object) {
        Kryo kryo = threadLocalKryo.get();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        Output output = new Output(os);
        kryo.writeObject(output, object);
        output.close();
        return os.toByteArray();
    }

    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        Kryo kryo = threadLocalKryo.get();
        Input input = new Input(new ByteArrayInputStream(bytes));
        T obj = kryo.readObject(input, clazz);
        input.close();
        return obj;
    }

    public void serializeToFile(Object object, File file) throws IOException {
        try (Output output = new Output(new FileOutputStream(file))) {
            threadLocalKryo.get().writeObject(output, object);
        }
    }

    public <T> T deserializeFromFile(File file, Class<T> clazz) throws IOException {
        try (Input input = new Input(new FileInputStream(file))) {
            return threadLocalKryo.get().readObject(input, clazz);
        }
    }

    @Override
    public void close() {
        threadLocalKryo.remove();
    }
    
}
