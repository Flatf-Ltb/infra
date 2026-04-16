package io.flatf.persistence.h2;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.flatf.common.file.FileUtil;

import javax.annotation.Nonnull;
import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class H2Connector {

    private static final String MemMode = "jdbc:h2:mem:";

    private static final String FileMode = "jdbc:h2:file:";

    private static final String TcpMode = "jdbc:h2:tcp:";

    private static final String TcpModeWithTls = "jdbc:h2:ssl:";

    private static final String DriverClass = "org.h2.Driver";

    private final String url;

    /**
     * memory private mode<br>
     * <br>
     * 被开启的数据库是私有的(private). URL为"jdbc:h2:mem:". <br>
     * 在同一个虚拟机中开启两个连接意味着打开两个不同的(private)数据库.
     *
     * @return H2Connector
     */
    public static H2Connector mem() {
        return new H2Connector(MemMode);
    }

    /**
     * memory named mode<br>
     * <br>
     * 其他应用可以通过使用命令来访问, 有时需要到同一个内存数据库的多个连接. <br>
     * 在这个场景中, 数据库URL必须包含一个名字. 例如："jdbc:h2:mem:example". <br>
     * 仅在同一个虚拟机和ClassLoader下可以通过这个URL访问到同样的数据库. <br>
     * <br>
     * 默认最后一个连接到数据库的连接关闭时就会关闭数据库. 对于一个内存数据库, 这意味着内容将会丢失. <br>
     * 为了保持数据库开启, 可以添加"DB_CLOSE_DELAY=-1"到数据库URL中. <br>
     * 为了让内存数据库的数据在虚拟机运行时始终存在, 请使用"jdbc:h2:mem:example;DB_CLOSE_DELAY=-1"
     *
     * @param named String
     * @return H2Connector
     */
    public static H2Connector mem(@Nonnull String named) {
        return new H2Connector(MemMode + named);
    }

    /**
     * 使用本地文件<br>
     * <br>
     * 连接到本地数据库的URL是"<b>jdbc:h2:[file:][path]{databaseName}</b>", <br>
     * 其中前缀"[file:]"是可选的.<br>
     * 如果没有设置路径或者只使用了相对路径, 则当前工作目录将被作为起点使用. <br>
     * 路径和数据库名称的大小写敏感取决于操作系统, 推荐只使用小写字母. <br>
     * 数据库名称必须最少三个字母(File.createTempFile的限制). 数据库名字不允许包含分号";". <br>
     * <br>
     * 可以使用"<b>~</b>"来指向当前用户home目录, 例如"jdbc:h2:~/sample".<br>
     * <br>
     * 文件模式下的本地文件连接URL的实例：<br>
     * <br>
     * jdbc:h2:file:/data/sample
     *
     * @param dbFile File
     * @return H2Connector
     */
    public static H2Connector file(@Nonnull File dbFile) {
        FileUtil.mkdir(dbFile);
        return new H2Connector(FileMode + dbFile.getAbsolutePath());
    }

    /**
     * @param dbFile File
     * @param addr   String
     * @return H2Connector
     */
    public static H2Connector tcpServer(@Nonnull File dbFile, String addr) {
        FileUtil.mkdir(dbFile);
        return new H2Connector(TcpMode + dbFile.getAbsolutePath());
    }

    /**
     * 使用TCP<br>
     * <br>
     * 格式如下, 通过指定server和port连接到以内嵌模式运行的H2服务器, 参数path和databaseName对应该内嵌数据库启动时的参数:<br>
     * jdbc:h2:tcp://${server}[:${port}]/[path]${databaseName>}<br>
     * <br>
     * 范例:<br>
     * jdbc:h2:tcp://localhost/~/test （对应内嵌模式下的jdbc:h2:~/test)<br>
     * jdbc:h2:tcp://dbserver:8084/~/sample （对应内嵌模式下的jdbc:h2:~/sample)<br>
     * jdbc:h2:tcp://localhost/mem:test （对应内嵌模式下的jdbc:h2:mem:test)<br>
     * <br>
     *
     * @param dbFile File
     * @param addr   String
     * @param port   int
     * @return H2Connector
     */
    public static H2Connector tcpServer(@Nonnull File dbFile, String addr, int port) {
        FileUtil.mkdir(dbFile);
        return new H2Connector(TcpMode + dbFile.getAbsolutePath());
    }

    /**
     * @param dbFile File
     * @param addr   String
     * @return H2Connector
     */
    public static H2Connector tcpServerWithTLS(@Nonnull File dbFile, String addr) {
        FileUtil.mkdir(dbFile);
        return new H2Connector(TcpModeWithTls + dbFile.getAbsolutePath());
    }

    /**
     * @param dbFile File
     * @param addr   String
     * @param port   int
     * @return H2Connector
     */
    public static H2Connector tcpServerWithTLS(@Nonnull File dbFile, String addr, int port) {
        FileUtil.mkdir(dbFile);
        return new H2Connector(TcpModeWithTls + dbFile.getAbsolutePath());
    }

    /**
     * @param url String
     */
    private H2Connector(String url) {
        this.url = url;
    }

    /**
     * @return String
     */
    public String getUrl() {
        return url;
    }

    public DataSource get() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setDriverClassName(DriverClass);
        return new HikariDataSource(config);
    }

    /**
     * 以嵌入式(本地)连接方式连接H2数据库
     */
    private static final String JDBC_URL = "jdbc:h2:file:~/h2-file/user";

    /**
     * 使用TCP/IP的服务器模式(远程连接)方式连接H2数据库(推荐)
     */
    // private static final String JDBC_URL =
    // "jdbc:h2:tcp://127.0.0.1/~/h2-file/user";

    private static final String USER = "root";
    private static final String PASSWORD = "root";

    public static void main(String[] args) throws Exception {

        // jdbc:h2:~/test
        // 连接位于用户目录下的test数据库
        // jdbc:h2:file:/data/sample

        // TODO Auto-generated method stub
        Class.forName(DriverClass);
        Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
        Statement statement = conn.createStatement();
        statement.execute("DROP TABLE IF EXISTS USER_INF");
        statement.execute("CREATE TABLE USER_INF(id INTEGER PRIMARY KEY, name VARCHAR(100), sex VARCHAR(2))");
        statement.executeUpdate("INSERT INTO USER_INF VALUES(1, 'tom', '男') ");
        statement.executeUpdate("INSERT INTO USER_INF VALUES(2, 'jack', '女') ");
        statement.executeUpdate("INSERT INTO USER_INF VALUES(3, 'marry', '男') ");
        statement.executeUpdate("INSERT INTO USER_INF VALUES(4, 'lucy', '男') ");

        ResultSet resultSet = statement.executeQuery("select * from USER_INF");

        while (resultSet.next()) {
            System.out.println(
                    resultSet.getInt("id")
                            + ", " + resultSet.getString("name")
                            + ", " + resultSet.getString("sex"));
        }

        statement.close();
        conn.close();
    }

}
