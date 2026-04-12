package io.flatf.transport.zmq;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import java.net.URL;

public class ZmqConfiguratorTest {

	@Test
	public void test() {

		System.out.println(ZmqProtocol.INPROC.addr("test"));
		System.out.println(ZmqProtocol.IPC.addr("str"));
		System.out.println(ZmqProtocol.TCP.addr("*:9889"));
		System.out.println(ZmqProtocol.TCP.addr("192.168.1.1:7887"));

		URL url = this.getClass().getResource("zmq-config.conf");
		System.out.println(url.getFile());

		Config config = ConfigFactory.parseURL(url);

		ZmqCfg configurator = ZmqCfg.config(config);

		System.out.println(configurator);

	}

}
