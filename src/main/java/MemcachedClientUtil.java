import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.command.BinaryCommandFactory;
import net.rubyeye.xmemcached.exception.MemcachedException;
import net.rubyeye.xmemcached.impl.ElectionMemcachedSessionLocator;
import net.rubyeye.xmemcached.impl.KetamaMemcachedSessionLocator;
import net.rubyeye.xmemcached.utils.AddrUtil;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

/**
 * Created by liuhonglin on 2015/8/4.
 */
public class MemcachedClientUtil {

    private static MemcachedClient client = null;

    private MemcachedClientUtil(){}

    public static void initCache() {
        if(client == null) {
            synchronized (MemcachedClientUtil.class) {
                if(client == null) {
                    final InetSocketAddress socketAddress1 = new InetSocketAddress("192.168.153.129", 11211);
                    final InetSocketAddress socketAddress2 = new InetSocketAddress("192.168.153.128", 11211);

                    //MemcachedClientBuilder builder = new XMemcachedClientBuilder(AddrUtil.getAddresses("192.168.153.129:11211 192.168.153.128:11211"), new int[]{2, 1});
                    MemcachedClientBuilder builder = new XMemcachedClientBuilder(
                                                            new ArrayList<InetSocketAddress>(){
                                                                {
                                                                    add(socketAddress1);
                                                                    add(socketAddress2);
                                                                }
                                                            },
                                                            new int[]{1, 1} //
                                                        );
                    builder.setCommandFactory(new BinaryCommandFactory());  // use binary protocol; default use TextCommandFactory，也就是文本协议
                    builder.setSessionLocator(new KetamaMemcachedSessionLocator()); // 设置客户端分布策略，一致性哈希（consistent hash)
                    //builder.setSessionLocator(new ElectionMemcachedSessionLocator());   // 设置客户端分布策略，选举散列,在某些场景下可以替代一致性哈希
                    builder.setConnectionPoolSize(100);   // Xmemcached是基于java nio的client实现，默认对一个memcached节点只有一个连接.XMemcached支持设置nio的连接池，允许建立多个连接到同一个memcached节点,这些连接之间是不同步的,需要自己保证数据更新的同步.

                    try {
                        client = builder.build();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    /**
     * 储存数据，默认永久有效（即30天）
     * @param key   key
     * @param value 可序列化的值
     * @return  true:储存成功，false:储存失败
     */
    public static boolean set(String key, Serializable value) {
        return set(key, 0, value);
    }

    /**
     *  储存数据
     * @param key    key
     * @param second 有效期，单位秒
     * @param value  可序列化的值
     * @return true:储存成功，false:储存失败
     */
    public static boolean set(String key, int second, Serializable value) {
        boolean result = false;
        try {
            result = client.set(key, second, value);
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (MemcachedException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Add key-value item to memcached, success only when the key is not exists in memcached.
     * @param key
     * @param value
     * @return
     */
    public static boolean add(String key, Serializable value) {
        return add(key, 0, value);
    }

    /**
     * Add key-value item to memcached, success only when the key is not exists in memcached.
     * @param key
     * @param second
     * @param value
     * @return
     */
    public static boolean add(String key, int second, Serializable value) {
        boolean result = false;
        try {
            result = client.add(key, second, value);
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (MemcachedException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Replace the key's data item in memcached,success only when the key's data item is exists in memcached.This method will wait for reply from server.
     * @param key
     * @param value
     * @return
     */
    public static boolean update(String key, Serializable value) {
        return update(key, 0, value);
    }

    /**
     * Replace the key's data item in memcached,success only when the key's data item is exists in memcached.This method will wait for reply from server.
     * @param key
     * @param second
     * @param value
     * @return
     */
    public static boolean update(String key, int second, Serializable value) {
        boolean result = false;
        try {
            result = client.replace(key, second, value);
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (MemcachedException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Get value by key
     * @param key
     * @return
     */
    public static Object get(String key) {
        Object value = null;
        try {
            value = client.get(key);
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (MemcachedException e) {
            e.printStackTrace();
        }
        return value;
    }

    /**
     * Delete key's data item from memcached.It it is not exists,return false.
     * @param key
     * @return
     */
    public static boolean delete(String key) {
        boolean result = false;
        try {
            result = client.delete(key);
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (MemcachedException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Append value to key's data item,this method will wait for reply
     * @param key
     * @param appendValue
     * @return
     */
    public static boolean append(String key, Serializable appendValue) {
        boolean result = false;
        try {
            result = client.append(key, appendValue);
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (MemcachedException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Prepend value to key's data item in memcached.
     * @param key
     * @param appendValue
     * @return
     */
    public static boolean prepend(String key, Serializable appendValue) {
        boolean result = false;
        try {
            result = client.prepend(key, appendValue);
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (MemcachedException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Set a new expiration time for an existing item,using default opTimeout second.
     * @param key
     * @param second
     * @return
     */
    public static boolean resetExpireTime(String key, int second) {
        boolean result = false;
        try {
            result = client.touch(key, second);
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (MemcachedException e) {
            e.printStackTrace();
        }
        return  result;
    }

    /**
     * Get item and set a new expiration time for it,using default opTimeout
     * 仅二进制协议支持
     * @param key
     * @param second
     * @return
     */
    public static Object getAndResetExpireTime(String key, int second) {
        Object value = null;
        try {
            value = client.getAndTouch(key, second);
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (MemcachedException e) {
            e.printStackTrace();
        }
        return value;
    }
}
