package com.nccc.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MySQLHelper {
	private static final Logger LOGGER = LoggerFactory.getLogger(MySQLHelper.class);
	
	private static final String DB_DRIVER;
	private static final String DB_URL;
	private static final String DB_USER;
	private static final String DB_PASSWORD;
	
	static{
		DB_DRIVER=PropertiesUtil.getPropertyString("mysql.db_driver");
		DB_URL=PropertiesUtil.getPropertyString("mysql.db_url");
		DB_USER=PropertiesUtil.getPropertyString("mysql.db_user");
		DB_PASSWORD=PropertiesUtil.getPropertyString("mysql.db_password");
		
	}

	private static Connection getConn() throws ClassNotFoundException, SQLException {
		Class.forName(DB_DRIVER);
		return DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
	}

	public static int executeUpdate(final String sql) throws SQLException, ClassNotFoundException {
		int result = -1;
		Statement stmt = null;
		Connection conn = null;
		try {
			conn = getConn();
			stmt = conn.createStatement();
			result = stmt.executeUpdate(sql);
		} catch (ClassNotFoundException e) {
			throw e;
		} catch (SQLException e) {
			throw e;
		} finally {
			close(conn, stmt, null);
		}
		return result;
	}

	public static int executeUpdate(String sql, Object[] params) throws SQLException, ClassNotFoundException {
		int result = -1;
		PreparedStatement stmt = null;
		Connection conn = null;
		try {
			conn = getConn();
			stmt = conn.prepareStatement(sql);
			System.out.println(sql + "|" + params.length);
			if (params != null) {
				int i = 0;
				for (Object obj : params) {
					stmt.setObject(i + 1, obj);
					i++;
				}
			}
			result = stmt.executeUpdate();
		} catch (ClassNotFoundException e) {
			throw e;
		} catch (SQLException e) {
			throw e;
		} finally {
			close(conn, stmt, null);
		}
		return result;
	}

	public static List<HashMap<String, Object>> ExecuteQuery(final String sql, Object[] params)
			throws Exception {
		List<HashMap<String, Object>> datas = null;
		PreparedStatement sta = null;
		ResultSet rs = null;
		Connection conn = getConn();
		try {
			sta = conn.prepareStatement(sql);
			if (params != null) {
				int i = 0;
				for (Object obj : params) {
					sta.setObject(i + 1, obj);
					i++;
				}
			}
			rs = sta.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int recount = rsmd.getColumnCount();
			String[] colLabels = new String[recount];
			for (int i = 0; i < recount; i++) {
				colLabels[i] = rsmd.getColumnLabel(i + 1);
			}
			datas = new ArrayList<HashMap<String, Object>>();
			while (rs.next()) {
				HashMap<String, Object> data = new HashMap<String, Object>();
				for (int i = 0; i < colLabels.length; i++) {
					data.put(colLabels[i], rs.getObject(colLabels[i]));
				}
				datas.add(data);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			close(conn, sta, rs);
		}
		return datas;
	}

	private static void close(final Connection conn, final Statement stmt, final ResultSet rs) {
		try {
			if (rs != null) {
				rs.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					if (conn != null) {
						conn.close();
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

		}
	}

//	public static void main(String[] args) throws ClassNotFoundException, SQLException {
//
//		String sql="insert into hosts(ip,hostname,username,pwd,des) values ('10.16.0.214','10.16.0.214','root','123456','ambari集群')";
//		MySQLHelper.executeUpdate(sql);
//		
//	}
	
	public static void main(String[] args) throws Exception{
		String sql="select * from hosts";
		List<HashMap<String, Object>> list=MySQLHelper.ExecuteQuery( sql, null);
		for(HashMap<String,Object> map:list){
			LOGGER.info("Hell");
			System.out.println("ip:"+map.get("ip")+" username:"+map.get("username")+" pwd:"+map.get("pwd"));
		}
		
	}

}
