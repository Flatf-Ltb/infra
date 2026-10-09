package net.openhft.chronicle.queue.simple.input;

import java.io.FileNotFoundException;

import net.openhft.chronicle.queue.main.DumpMain;

/**
 * Created by catherine on 18/07/2016.
 */
public class PrintQueueMain {

	public static void main(String[] args) throws FileNotFoundException {

		DumpMain.dump("queue");

	}
}
