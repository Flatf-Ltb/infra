package io.flatf.infra.persistence.h2;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

/**
 * Created by sky on 16-1-18.
 */
public class AddressTest extends H2TestBase {

	private Connection connection;

	@Before
	private void before() throws SQLException {
		connection = dataSource.getConnection();
	}

	@Test
	public void test0() throws Exception {
		try (PreparedStatement statement0 = connection
				.prepareStatement("INSERT INTO address(id, name) VALUES(1, 'Miller');");
				PreparedStatement statement1 = connection.prepareStatement("select id, name from address");
				ResultSet executeQuery = statement1.executeQuery();) {

			assertEquals(1, statement0.executeUpdate());

			executeQuery.next();
			int id = executeQuery.getInt(1);
			String name = executeQuery.getString(2);

			assertEquals(1, id);
			assertEquals("Miller", name);

			System.out.println("get data from database: id=" + id + ", name=" + name);
		}

	}

	@After
	private void after() {
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
