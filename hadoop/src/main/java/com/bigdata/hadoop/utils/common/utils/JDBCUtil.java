package com.bigdata.hadoop.utils.common.utils;


import com.bigdata.hadoop.utils.common.sysConstants.SysConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBCUtil
{

    public static final Logger logger = LoggerFactory.getLogger(JDBCUtil.class);
    private Connection connection = null;
    private Statement statement = null;
    private ResultSet resultSet = null;
    private PreparedStatement preparedStatement ;
    private static String JDBCClassName = null;
    private static String JDBCUrl = null;
    private static String JDBCUser = null;
    private static String JDBCPassword  = null;
    private static JDBCUtil instance = null;
    private static String connectSuffix = "?useUnicode=true&characterEncoding=utf-8&useSSL=false";
    protected JDBCUtil()
    {
    }
    /**
     * @author libangqin
     * 静态加载从配置文件初始化连接信息
     * */
    static
    {
        try
        {
            JDBCClassName = PropertiesUtil.getPropertyByKey("jdbc.driver", SysConstants.configPath);
            JDBCUrl = PropertiesUtil.getPropertyByKey("jdbc.url", SysConstants.configPath);
            JDBCUser = PropertiesUtil.getPropertyByKey("jdbc.user", SysConstants.configPath);
            JDBCPassword = PropertiesUtil.getPropertyByKey("jdbc.password", SysConstants.configPath);
            Class.forName(JDBCClassName);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static synchronized JDBCUtil getInstance()
    {
        if(instance == null)
        {
            synchronized (JDBCUtil.class) {
                if(instance==null)
                {
                    instance = new JDBCUtil();
                }
            }
        }
        return instance;
    }

    public  synchronized Connection getConnection() throws SQLException
    {
        //仅当connection失效时才重新获取
        if (connection == null ) //isValid(10)方法在此无效 与版本有关
        {
            connection = DriverManager.getConnection(JDBCUrl + connectSuffix, JDBCUser, JDBCPassword);
        }
        if(connection!=null)
        {
            return connection;
        }
        else
        {
            logger.warn("error empty connection");
            return connection ;
        }
    }

    public synchronized Statement getStatement(Connection connection) throws SQLException
    {
        getConnection();
        //仅当statement失效时才重新创建
        if (statement == null || statement.isClosed() == true)
        {
            statement = connection.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
        }
        if(statement!=null)
        {
            return statement ;
        }
        else
        {
            logger.warn("that statement is null ");
            return null ;
        }
    }

    public synchronized void close() throws SQLException
    {
        logger.info("close connection now !");
        if (resultSet != null)
        {
            resultSet.close();
            resultSet = null;
        }
        if (statement != null)
        {
            statement.close();
            statement = null;
        }
        if (connection != null)
        {
            connection.close();
            connection = null;
        }
    }


    /**
     * @author libangqin
     * @detail 获取查询结果集ResultSet 使用statement 直接查询
     * */
    public synchronized ResultSet executeQuery(String sql,Statement statement) throws SQLException
    {
        if (resultSet != null && resultSet.isClosed() == false)
        {
            resultSet.close();
        }
        resultSet = null;
        resultSet = statement.executeQuery(sql);
        this.close();
        return resultSet;
    }

    public synchronized int executeUpdate(String sql,List<Object>paramsList) throws SQLException
    {
        int result = 0 ;
        preparedStatement = this.getConnection().prepareStatement(sql);
        if (paramsList != null&&paramsList.size()>0) {
for (int i = 0; i < paramsList.size(); i++) {
preparedStatement.setObject(i + 1, paramsList.get(i));
}
}
        result = preparedStatement.executeUpdate();
        this.close();
        return result ;
    }

    public synchronized int executeUpdate(String sql) throws SQLException
    {
        int result = 0;
        result = this.getStatement(this.getConnection()).executeUpdate(sql);
        this.close();
        return result;
    }

    /**
* SQL 查询将查询结果直接放入ResultSet中
* @param sql SQL语句
* @param
* @return 结果集
*/
private ResultSet executeQueryRS(String sql, List<Object> paramsList) {
try {
// 获得连接
connection = this.getConnection();

// 调用SQL
preparedStatement = connection.prepareStatement(sql);

// 参数赋值
if (paramsList != null&&paramsList.size()>0) {
for (int i = 0; i < paramsList.size(); i++) {
preparedStatement.setObject(i + 1, paramsList.get(i));
}
}

// 执行
resultSet = preparedStatement.executeQuery();

} catch (SQLException e) {
logger.error(e.getMessage());
}

return resultSet;
}


     /**
* 获取查询结果集，并将结果放在List中
* @author libangqin
* @param sql
*            SQL语句
* @return List<Map<String,Object>>
*            结果集
*/
public synchronized List<Map<String,Object>> excuteQuery(String sql, List<Object> paramsList) {
// 执行SQL获得结果集
ResultSet rs = executeQueryRS(sql, paramsList);

// 创建ResultSetMetaData对象
ResultSetMetaData rsmd = null;

// 结果集列数
int columnCount = 0;
try {
rsmd = rs.getMetaData();

// 获得结果集列数
columnCount = rsmd.getColumnCount();
} catch (SQLException ex) {
logger.error(ex.getMessage());
}

// 创建List
List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();

try {
// 将ResultSet的结果保存到List中
while (rs.next()) {
Map<String, Object> map = new HashMap<String, Object>();
for (int i = 1; i <= columnCount; i++) {
map.put(rsmd.getColumnLabel(i), rs.getObject(i));
}
resultList.add(map);
}
} catch (SQLException e) {
logger.error(e.getMessage());
} finally {
// 关闭所有资源
try {
                close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
}
return resultList;
}
}
