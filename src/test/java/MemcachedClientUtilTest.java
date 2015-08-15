import org.junit.Assert;
import org.junit.Test;

/**
 * Created by liuhonglin on 2015/8/4.
 */
public class MemcachedClientUtilTest {

    @Test
    public void testGet() {
        MemcachedClientUtil.initCache();
        Assert.assertEquals(MemcachedClientUtil.get("key1"), "value1");
    }

    @Test
    public void testSet() {
        MemcachedClientUtil.initCache();
        Assert.assertEquals(MemcachedClientUtil.set("key1", "value1"), true);
    }

    public static void main(String[] args) {
        MemcachedClientUtil.initCache();
        MemcachedClientUtil.initCache();
        MemcachedClientUtil.set("key1", "value1");
    }
}
