package io.flatf.foundation.configuration.nacos;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import io.flatf.foundation.common.util.PropertiesUtil;
import io.flatf.foundation.common.util.StringSupport;

import java.io.IOException;
import java.util.Properties;

/**
 * NACOS信息读取
 * 使用NACOS读取properties配置文件
 *
 * @author : yellow013
 */
public class NacosReader {

    /**
     * @param serverAddr String
     * @param group      String
     * @param dataId     String
     * @return Properties
     * @throws NacosConnectionException e0
     * @throws NacosReadException       e0
     */
    public static Properties getProperties(String serverAddr, String group, String dataId)
            throws NacosConnectionException, NacosReadException {
        return getProperties0(connection(serverAddr), group, dataId);
    }

    /**
     * @param serverAddr String
     * @param namespace  String
     * @param group      String
     * @param dataId     String
     * @return Properties
     * @throws NacosConnectionException e0
     * @throws NacosReadException       e1
     */
    public static Properties getProperties(String serverAddr, String namespace,
                                           String group, String dataId)
            throws NacosConnectionException, NacosReadException {
        return getProperties0(connection(serverAddr, namespace), group, dataId);
    }

    /**
     * @param serverAddr String
     * @param group      String
     * @param dataId     String
     * @return String
     * @throws NacosConnectionException e0
     * @throws NacosReadException       e1
     */
    public static String getSaved(String serverAddr, String group, String dataId)
            throws NacosConnectionException, NacosReadException {
        return getSaved0(connection(serverAddr), group, dataId);
    }

    /**
     * @param serverAddr String
     * @param namespace  String
     * @param group      String
     * @param dataId     String
     * @return String
     * @throws NacosConnectionException e0
     * @throws NacosReadException       e1
     */
    public static String getSaved(String serverAddr, String namespace,
                                  String group, String dataId)
            throws NacosConnectionException, NacosReadException {
        return getSaved0(connection(serverAddr, namespace), group, dataId);
    }

    private static ConfigService connection(String serverAddr)
            throws NacosConnectionException {
        Properties prop = new Properties();
        prop.put(PropertyKeyConst.SERVER_ADDR, serverAddr);
        return createConfigService(prop);
    }

    private static ConfigService connection(String serverAddr, String namespace)
            throws NacosConnectionException {
        Properties prop = new Properties();
        prop.put(PropertyKeyConst.SERVER_ADDR, serverAddr);
        prop.put(PropertyKeyConst.NAMESPACE, namespace);
        return createConfigService(prop);
    }

    private static ConfigService createConfigService(Properties prop)
            throws NacosConnectionException {
        try {
            return ConfigFactory.createConfigService(prop);
        } catch (NacosException e) {
            throw new NacosConnectionException("ConfigFactory.createConfigService has error", e);
        }
    }

    private static Properties getProperties0(ConfigService service,
                                             String group, String dataId)
            throws NacosReadException {
        String saved = getSaved0(service, group, dataId);
        try {
            return PropertiesUtil.load(saved);
        } catch (IOException e) {
            throw new NacosReadException(e);
        }
    }

    private static String getSaved0(ConfigService service, String group, String dataId)
            throws NacosReadException {
        try {
            String saved = service.getConfig(dataId, group, 100000);
            if (StringSupport.isNullOrEmpty(saved)) {
                throw new NacosReadException("Read nacos saved is null or empty");
            }
            return saved;
        } catch (NacosException e) {
            throw new NacosReadException("ConfigService.getConfig call error", e);
        }
    }

}
