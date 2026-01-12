package io.mercury.serialization.fory;

import org.apache.fory.Fory;
import org.apache.fory.config.Language;

public final class ForyKeeper {

    private ForyKeeper() {
    }

    /**
     * @param classes Class<?> array
     * @return Fory
     */
    public static Fory newInstance(Class<?>... classes) {
        return newInstance(Language.JAVA, classes);
    }

    /**
     * @param lang    Language
     * @param classes Class<?> array
     * @return Fory
     */
    public static Fory newInstance(Language lang, Class<?>... classes) {
        var fory = Fory.builder()
                .withLanguage(lang)
                .requireClassRegistration(true)
                .build();
        for (var clazz : classes)
            fory.register(clazz);
        return fory;
    }

}
