package com.zifang.z.etl.core.connector;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * 驱动直连数据源（每次 {@link #getConnection()} 现开现关，不池化）。
 *
 * <p>数据源登记表解析用：z-etl 的管控面是低频搬运（cron 批次 / 增量轮询），
 * 不内置连接池；宿主要池化自己包一层即可（DataSourceProvider SPI 不变）。
 * 驱动类由 JDBC url 自动发现（JDBC 4 SPI），本类不显式 {@code Class.forName}。
 *
 * @author zifang
 */
public class DriverUrlDataSource implements DataSource {

    private final String url;
    private final String username;
    private final String password;

    public DriverUrlDataSource(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public String getUrl() {
        return url;
    }

    @Override
    public Connection getConnection() throws SQLException {
        if (username == null || username.isEmpty()) {
            return DriverManager.getConnection(url);
        }
        Properties props = new Properties();
        props.setProperty("user", username);
        if (password != null) {
            props.setProperty("password", password);
        }
        return DriverManager.getConnection(url, props);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("Not a wrapper for " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    @Override
    public PrintWriter getLogWriter() {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) {
        // 无日志.writer 口径
    }

    @Override
    public void setLoginTimeout(int seconds) {
        DriverManager.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() {
        return DriverManager.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("getParentLogger 不支持");
    }
}
