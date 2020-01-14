package org.activiti.engine.impl.interceptor;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;
import org.activiti.engine.impl.persistence.entity.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.binding.MapperMethod;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.reflection.DefaultReflectorFactory;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.factory.DefaultObjectFactory;
import org.apache.ibatis.reflection.wrapper.DefaultObjectWrapperFactory;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.springframework.beans.BeanUtils;
import org.springframework.util.ClassUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;


/**
 * User : linqi
 * Date : 2019/12/24
 */
@Intercepts({
        @Signature(type = ParameterHandler.class, method = "setParameters", args = PreparedStatement.class),
        @Signature(type = Executor.class, method = "update", args = {MappedStatement.class, Object.class}),
        @Signature(type = Executor.class, method = "query",
                args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class, CacheKey.class, BoundSql.class}),
        @Signature(type = Executor.class, method = "query",
                args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class}),
        @Signature(type = Executor.class, method = "createCacheKey", args = {MappedStatement.class, Object.class, RowBounds.class, BoundSql.class})
}
)
public class ParameterInterceptor implements Interceptor {
    //@Value("${udal.schema.shardingNum}")
    public static int shardingNum;
    //@Value("${udal.schema.name}")
    public static String schemaName;
    private static final String PARAM_KEY = "companyId";
    //private static String[] globalTable = new String[]{"sys_dict", "act_procdef_info", "act_ru_event_subscr", "act_ge_property"};
    private static String[] shardingTable = new String[]{"ACT_GE_BYTEARRAY", "ACT_RE_DEPLOYMENT", "ACT_RE_MODEL", "ACT_RE_PROCDEF", "ACT_RU_EXECUTION", "ACT_RU_IDENTITYLINK", "ACT_RU_TASK", "ACT_RU_VARIABLE"};

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        if (shardingNum == 0 && RequestHolder.udalConfMap.size() > 0) {
            //获取udal配置
            shardingNum = Integer.valueOf(RequestHolder.udalConfMap.get("shardingNum"));
            schemaName = RequestHolder.udalConfMap.get("schemaName");
        }
        //拦截 ParameterHandler 的 setParameters 方法 动态设置参数
        if (invocation.getTarget() instanceof ParameterHandler) {
            return invokeSetParameter(invocation);
        } else {
            // 获取第一个参数
            MappedStatement ms = (MappedStatement) invocation.getArgs()[0];
            // insert 语句 不处理
            if (ms.getSqlCommandType() == SqlCommandType.INSERT) {
                return invocation.proceed();
            }
            String sql = getSqlByInvocation(invocation);
            Set<String> tableSet = this.getTableNames(sql);
            if (tableSet != null && tableSet.size() > 0) {
                for (String table : tableSet) {
                    if (table.equals("ACT_RU_EXECUTION")) {
                        System.out.println("");
                    }
                    System.out.println("Executor拦截到sql：" + sql + " ，关联表：" + table);
                    if (RequestHolder.getId() == null || !Arrays.asList(shardingTable).contains(table)) {
                        System.out.println("不做处理，没有租户||全局表或者单片表：" + sql + " ，表：" + table);
                        return invocation.proceed();
                    }
                    if (!sql.contains("insert") || !sql.contains("INSERT")) {
                        sql = hintTableSchema(sql);
                        resetSql2Invocation(invocation, sql);
                        // 返回，继续执行
                        return invocation.proceed();
                    }
                }
            }
            // 返回，继续执行
            return invocation.proceed();
        }
        //return null;
    }

    private Object invokeCacheKey(Invocation invocation) throws Exception {
        Executor executor = (Executor) invocation.getTarget();
        MappedStatement ms = (MappedStatement) invocation.getArgs()[0];
        if (ms.getSqlCommandType() != SqlCommandType.SELECT) {
            return invocation.proceed();
        }

        // insert 语句 不处理
        if (ms.getSqlCommandType() == SqlCommandType.INSERT) {
            return invocation.proceed();
        }

        // 返回，继续执行
        return invocation.proceed();
//        List<String> paramNames = new ArrayList<>();
//        boolean hasKey = hasParamKey(paramNames, boundSql.getParameterMappings());
//
//        if (!hasKey) {
//            return invocation.proceed();
//        }
//        // 改写参数
//        parameterObject = processSingle(parameterObject, paramNames);
//
//        return executor.createCacheKey(ms, parameterObject, rowBounds, boundSql);
    }

    private Object invokeUpdate(Invocation invocation) throws Exception {
        Executor executor = (Executor) invocation.getTarget();
        // 获取第一个参数
        MappedStatement ms = (MappedStatement) invocation.getArgs()[0];
        // insert 语句 不处理
        if (ms.getSqlCommandType() == SqlCommandType.INSERT) {
            return invocation.proceed();
        }
        String sql = getSqlByInvocation(invocation);
        sql = hintTableSchema(sql);
        resetSql2Invocation(invocation, sql);
        // 返回，继续执行
        return invocation.proceed();

    }


    private Object invokeSetParameter(Invocation invocation) throws Exception {
        System.out.println("activiti 当前处理线程id：" + Thread.currentThread().getId() + ",companyId=" + RequestHolder.getId());
        if (RequestHolder.getId() == null)
            return invocation.proceed();
        ParameterHandler parameterHandler = (ParameterHandler) invocation.getTarget();
        PreparedStatement ps = (PreparedStatement) invocation.getArgs()[0];

        // 反射获取 BoundSql 对象，此对象包含生成的sql和sql的参数map映射
        Field boundSqlField = parameterHandler.getClass().getDeclaredField("boundSql");
        boundSqlField.setAccessible(true);
        BoundSql boundSql = (BoundSql) boundSqlField.get(parameterHandler);
        String sql = boundSql.getSql();
        Set<String> tableSet = this.getTableNames(boundSql.getSql());
        if (tableSet != null && tableSet.size() > 0) {
            for (String table : tableSet) {
                if (table.equals("ACT_RU_EXECUTION")) {
                    System.out.println("");
                }
                System.out.println("拦截到sql：" + boundSql.getSql() + " ，关联表：" + table);
                if (!Arrays.asList(shardingTable).contains(table)) {
                    return invocation.proceed();
                }
                if (!sql.contains("insert") && !sql.contains("INSERT")) {
//                    sql = hintTableSchema(sql);
//                    resetSql2Invocation(invocation,sql);
                    // 返回，继续执行
                    return invocation.proceed();
                }
            }
        }
//        if(tableSet.contains("ACT_ID_USER"))
//            System.out.println("breakPoint");
        List<String> paramNames = new ArrayList<>();
        // 若参数映射没有包含的key直接返回
        boolean hasKey = hasParamKey(paramNames, boundSql.getParameterMappings());
        if (!hasKey) {
            return invocation.proceed();
        }

        // 反射获取 参数对像
        Field parameterField = parameterHandler.getClass().getDeclaredField("parameterObject");
        parameterField.setAccessible(true);
        Object parameterObject = parameterField.get(parameterHandler);

        // 改写参数
        parameterObject = processSingle(parameterObject, paramNames);

        // 改写的参数设置到原parameterHandler对象
        parameterField.set(parameterHandler, parameterObject);
        parameterHandler.setParameters(ps);
        return null;
    }

    // 判断已生成sql参数映射中是否包含tenantId
    private boolean hasParamKey(List<String> paramNames, List<ParameterMapping> parameterMappings) {
        boolean hasKey = false;
        for (ParameterMapping parameterMapping : parameterMappings) {
            if (StringUtils.equals(parameterMapping.getProperty(), PARAM_KEY) || parameterMapping.getProperty().contains(PARAM_KEY)) {
                hasKey = true;
            } else {
                paramNames.add(parameterMapping.getProperty());
            }
        }
        return hasKey;
    }

    private Object processSingle(Object paramObj, List<String> paramNames) throws Exception {

        Map<String, Object> paramMap = new MapperMethod.ParamMap<>();
        if (paramObj == null) {
            //paramMap.put(PARAM_KEY, RequestHolder.getId());
            paramMap.put(PARAM_KEY, RequestHolder.getId());
            paramObj = paramMap;
            // 单参数 将 参数转为 map
        } else if (ClassUtils.isPrimitiveOrWrapper(paramObj.getClass())
                || String.class.isAssignableFrom(paramObj.getClass())
                || Number.class.isAssignableFrom(paramObj.getClass())) {
            if (paramNames.size() == 1) {
                paramMap.put(paramNames.iterator().next(), paramObj);
                //paramMap.put(PARAM_KEY, RequestHolder.getId());
                paramMap.put(PARAM_KEY, RequestHolder.getId());
                paramObj = paramMap;
            }
        } else {
            processParam(paramObj);
        }

        return paramObj;
    }

    public static void processParam(Object parameterObject) throws IllegalAccessException, InvocationTargetException {
        if (parameterObject instanceof Map) {
            ((Map) parameterObject).putIfAbsent(PARAM_KEY, RequestHolder.getId());

            List<Object> list = (List<Object>) ((Map) parameterObject).get("list");
            List<Object> newList = new LinkedList<>();
            for (Object object : list) {
                String entity = object.getClass().getSimpleName();
                // 后续发现批量插入则继续添加
                switch (entity) {
                    case "ResourceEntity":
                        ResourceEntity resourceEntity = (ResourceEntity) object;
                        resourceEntity.setCompanyId(RequestHolder.getId());
                        newList.add(resourceEntity);
                        break;
                    case "VariableInstanceEntity":
                        VariableInstanceEntity variableInstanceEntity = (VariableInstanceEntity) object;
                        variableInstanceEntity.setCompanyId(RequestHolder.getId());
                        newList.add(variableInstanceEntity);
                        break;
                    case "ExecutionEntity":
                        ExecutionEntity executionEntity = (ExecutionEntity) object;
                        executionEntity.setCompanyId(RequestHolder.getId());
                        newList.add(executionEntity);
                        break;
                    case "ByteArrayEntity":
                        ByteArrayEntity byteArrayEntity = (ByteArrayEntity) object;
                        byteArrayEntity.setCompanyId(RequestHolder.getId());
                        newList.add(byteArrayEntity);
                        break;
                    case "ProcessDefinitionEntity":
                        ProcessDefinitionEntity processDefinitionEntity = (ProcessDefinitionEntity) object;
                        processDefinitionEntity.setCompanyId(RequestHolder.getId());
                        newList.add(processDefinitionEntity);
                        break;
                    case "IdentityLinkEntity":
                        IdentityLinkEntity identityLinkEntity = (IdentityLinkEntity) object;
                        identityLinkEntity.setCompanyId(RequestHolder.getId());
                        newList.add(identityLinkEntity);
                        break;
                    case "TaskEntity":
                        TaskEntity taskEntity = (TaskEntity) object;
                        taskEntity.setCompanyId(RequestHolder.getId());
                        newList.add(taskEntity);
                        break;
                    case "DeploymentEntity":
                        DeploymentEntity deploymentEntity = (DeploymentEntity) object;
                        deploymentEntity.setCompanyId(RequestHolder.getId());
                        newList.add(deploymentEntity);
                        break;
                    case "ModelEntity":
                        ModelEntity modelEntity = (ModelEntity) object;
                        modelEntity.setCompanyId(RequestHolder.getId());
                        newList.add(modelEntity);
                        break;
                }
            }
            ((Map) parameterObject).put("list", newList);


//            List<User> users = (List<User>) ((Map) parameterObject).get("list");
//            users.stream().forEach(user->user.setTenantId(RequestHolder.getId()));
//            ((Map) parameterObject).put("list",users);
        } else {
            PropertyDescriptor ps = BeanUtils.getPropertyDescriptor(parameterObject.getClass(), PARAM_KEY);
            if (ps != null && ps.getReadMethod() != null && ps.getWriteMethod() != null) {
                Object value = ps.getReadMethod().invoke(parameterObject);
                if (value == null) {
                    //ps.getWriteMethod().invoke(parameterObject, RequestHolder.getId());
                    ps.getWriteMethod().invoke(parameterObject, RequestHolder.getId());
                }
            }
        }
    }

    /**
     * @param sql
     * @return 获取sql所有的表名
     */
    private Set<String> getTableNames(String sql) throws Exception {
        String dbType = JdbcConstants.MYSQL;

        //格式化输出
        String result = SQLUtils.format(sql, dbType);
        System.out.println(result); // 缺省大写格式
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        Set<String> tableSet = new LinkedHashSet<>();

        //解析出的独立语句的个数
        System.out.println("size is:" + stmtList.size());
        for (int i = 0; i < stmtList.size(); i++) {
            SQLStatement stmt = stmtList.get(i);
            MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
            stmt.accept(visitor);

            //获取操作方法名称,依赖于表名称
            System.out.println("Manipulation : " + visitor.getTables());
            Map<TableStat.Name, TableStat> tableStatMap = visitor.getTables();
            for (TableStat.Name name : tableStatMap.keySet()) {
                tableSet.add(name.toString());
            }
            //获取字段名称
            System.out.println("fields : " + visitor.getColumns());
        }
        return tableSet;
    }


    @Override
    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }

    @Override
    public void setProperties(Properties properties) {

    }

    /**
     * @param sql
     * @return hint到租户对应的分库，先写死测试
     */
    public String hintTableSchema(String sql) throws Exception {

        if (RequestHolder.getId() == null || sql.contains("!HINT")) {
            return sql;
        } else {
            // 全局表也不需要拼hint
            Set<String> tableSet = getTableNames(sql);
            if (tableSet != null && tableSet.size() > 0) {
                for (String table : tableSet) {
                    if (!Arrays.asList(shardingTable).contains(table)) {
                        return sql;
                    }
                }
            }
            String hintStr = "";
            //分片先写死2，后面通过threadlocal透传
            if (shardingNum == 0)
                hintStr = String.format("/* !HINT({\"dn\":[\"workflowSchema_%d\"]})*/", RequestHolder.getId().hashCode() % 2 + 1);
            else
                hintStr = String.format("/* !HINT({\"dn\":[\"%s_%d\"]})*/", schemaName, RequestHolder.getId().hashCode() % shardingNum + 1);

            if (!hintStr.isEmpty()) {
                return hintStr + sql;
            }
        }
        return sql;
    }

    private String getOperateType(Invocation invocation) {
        final Object[] args = invocation.getArgs();
        MappedStatement ms = (MappedStatement) args[0];
        SqlCommandType commondType = ms.getSqlCommandType();
        if (commondType.compareTo(SqlCommandType.SELECT) == 0) {
            return "select";
        }
        if (commondType.compareTo(SqlCommandType.INSERT) == 0) {
            return "insert";
        }
        if (commondType.compareTo(SqlCommandType.UPDATE) == 0) {
            return "update";
        }
        if (commondType.compareTo(SqlCommandType.DELETE) == 0) {
            return "delete";
        }
        return null;
    }


    /**
     * 获取sql语句
     *
     * @param invocation
     * @return
     */
    public static String getSqlByInvocation(Invocation invocation) {
        final Object[] args = invocation.getArgs();
        MappedStatement ms = (MappedStatement) args[0];
        Object parameterObject = args[1];
        BoundSql boundSql = ms.getBoundSql(parameterObject);
        return boundSql.getSql();
    }

    /**
     * 包装sql后，重置到invocation中
     *
     * @param invocation
     * @param sql
     * @throws SQLException
     */
    public void resetSql2Invocation(Invocation invocation, String sql) throws SQLException {
        System.out.println("替换新sql:" + sql);
        final Object[] args = invocation.getArgs();
        MappedStatement statement = (MappedStatement) args[0];
        Object parameterObject = args[1];
        BoundSql boundSql = statement.getBoundSql(parameterObject);
        MappedStatement newStatement = newMappedStatement(statement, new BoundSqlSqlSource(boundSql));
        MetaObject msObject = MetaObject.forObject(newStatement, new DefaultObjectFactory(), new DefaultObjectWrapperFactory(), new DefaultReflectorFactory());
        msObject.setValue("sqlSource.boundSql.sql", sql);
        args[0] = newStatement;
    }


    public MappedStatement newMappedStatement(MappedStatement ms, SqlSource newSqlSource) {
        MappedStatement.Builder builder =
                new MappedStatement.Builder(ms.getConfiguration(), ms.getId(), newSqlSource, ms.getSqlCommandType());
        builder.resource(ms.getResource());
        builder.fetchSize(ms.getFetchSize());
        builder.statementType(ms.getStatementType());
        builder.keyGenerator(ms.getKeyGenerator());
        if (ms.getKeyProperties() != null && ms.getKeyProperties().length != 0) {
            StringBuilder keyProperties = new StringBuilder();
            for (String keyProperty : ms.getKeyProperties()) {
                keyProperties.append(keyProperty).append(",");
            }
            keyProperties.delete(keyProperties.length() - 1, keyProperties.length());
            builder.keyProperty(keyProperties.toString());
        }
        builder.timeout(ms.getTimeout());
        builder.parameterMap(ms.getParameterMap());
        builder.resultMaps(ms.getResultMaps());
        builder.resultSetType(ms.getResultSetType());
        builder.cache(ms.getCache());
        builder.flushCacheRequired(ms.isFlushCacheRequired());
        builder.useCache(ms.isUseCache());

        return builder.build();
    }

    //    定义一个内部辅助类，作用是包装sq
    class BoundSqlSqlSource implements SqlSource {
        private BoundSql boundSql;

        public BoundSqlSqlSource(BoundSql boundSql) {
            this.boundSql = boundSql;
        }

        @Override
        public BoundSql getBoundSql(Object parameterObject) {
            return boundSql;
        }
    }
}
