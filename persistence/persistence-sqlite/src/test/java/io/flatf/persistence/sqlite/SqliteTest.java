package io.flatf.persistence.sqlite;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Unit test for simple App.
 */
public final class SqliteTest {

	private Connection connection;
	private Statement statement;

	/**
	 * 连接到一个现有的数据库, 如果数据库不存在, 那么它就会被创建, 最后将返回一个数据库对象.
	 */
	@Before
	public void before() {
		try {
			Class.forName("org.sqlite.JDBC");
			connection = DriverManager.getConnection("jdbc:sqlite:C:\\db\\test.db");
			System.out.println("Opened database successfully");
			statement = connection.createStatement();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@After
	public void after() {
		try {
			statement.close();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void createTable() throws SQLException {
		String sql = "CREATE TABLE COMPANY "
				+ "(ID INT PRIMARY KEY NOT NULL," 
				+ " NAME TEXT NOT NULL,"
				+ " AGE INT NOT NULL," 
				+ " ADDRESS CHAR(50)," 
				+ " SALARY REAL)";
		statement.executeUpdate(sql);
	}

	@Test
	public void insert() throws SQLException {
		connection.setAutoCommit(false);
		StringBuffer sb = new StringBuffer();
		sb.append("INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) "
				+ "VALUES (1, 'Paul', 32, 'California', 20000.00 );\n");
		sb.append(
				"INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) " + "VALUES (2, 'Allen', 25, 'Texas', 15000.00 );\n");
		sb.append("INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) "
				+ "VALUES (3, 'Teddy', 23, 'Norway', 20000.00 );\n");
		sb.append("INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) "
				+ "VALUES (4, 'Mark', 25, 'Rich-Mond ', 65000.00 );");
		statement.executeUpdate(sb.toString());
		connection.commit();
	}

	@Test
	public void select() throws SQLException {
		try (ResultSet rs = statement.executeQuery("SELECT * FROM COMPANY;")) {
			while (rs.next()) {
				int id = rs.getInt("id");
				String name = rs.getString("name");
				int age = rs.getInt("age");
				String address = rs.getString("address");
				float salary = rs.getFloat("salary");
				System.out.println("ID = " + id);
				System.out.println("NAME = " + name);
				System.out.println("AGE = " + age);
				System.out.println("ADDRESS = " + address);
				System.out.println("SALARY = " + salary);
				System.out.println("--------");
			}
		}
	}

	@Test
	public void update() throws SQLException {
		connection.setAutoCommit(false);
		String sql = "UPDATE COMPANY set SALARY = 25000.00 where ID=1;";
		statement.executeUpdate(sql);
		connection.commit();

		ResultSet rs = statement.executeQuery("SELECT * FROM COMPANY;");
		while (rs.next()) {
			int id = rs.getInt("id");
			String name = rs.getString("name");
			int age = rs.getInt("age");
			String address = rs.getString("address");
			float salary = rs.getFloat("salary");
			System.out.println("ID = " + id);
			System.out.println("NAME = " + name);
			System.out.println("AGE = " + age);
			System.out.println("ADDRESS = " + address);
			System.out.println("SALARY = " + salary);
			System.out.println("--------");
		}
		rs.close();
	}

	@Test
	public void delete() throws SQLException {
		connection.setAutoCommit(false);
		String sql = "DELETE from COMPANY where ID=2;";
		statement.executeUpdate(sql);
		connection.commit();

		ResultSet rs = statement.executeQuery("SELECT * FROM COMPANY;");
		while (rs.next()) {
			int id = rs.getInt("id");
			String name = rs.getString("name");
			int age = rs.getInt("age");
			String address = rs.getString("address");
			double salary = rs.getDouble("salary");
			System.out.println("ID = " + id);
			System.out.println("NAME = " + name);
			System.out.println("AGE = " + age);
			System.out.println("ADDRESS = " + address);
			System.out.println("SALARY = " + salary);
			System.out.println("--------");
		}
		rs.close();
	}

}
