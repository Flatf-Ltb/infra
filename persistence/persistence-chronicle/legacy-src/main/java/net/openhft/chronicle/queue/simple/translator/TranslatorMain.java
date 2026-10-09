package net.openhft.chronicle.queue.simple.translator;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

/**
 * Created by catherine on 26/07/2016.
 */
public class TranslatorMain {

    public static void main(String[] args) throws InterruptedException {
        String path_fr = "queue-fr";
        String path_en = "queue-en";
        try (ChronicleQueue queue_fr = SingleChronicleQueueBuilder.binary(path_fr).build();
             ChronicleQueue queue_en = SingleChronicleQueueBuilder.binary(path_en).build()) {
            MessageConsumer messageConsumer = queue_fr.methodWriter(MessageConsumer.class);
            MessageConsumer simpleTranslator = new SimpleTranslator(messageConsumer);
            MethodReader methodReader = queue_en.createTailer().methodReader(simpleTranslator);
            while (true)
                if (!methodReader.readOne())
                    Thread.sleep(10);
        }
    }

}
