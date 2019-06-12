import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import utils.RowMapper;

public class UserRowMapper implements RowMapper<User> {
    private static byte[] COLUMNFAMILY1= "msg1".getBytes();
    private static byte[] COLUMNFAMILY2= "msg2".getBytes();
    private static byte[] NAME = "name".getBytes();
    private static byte[] INFO = "info".getBytes();
    public User mapRow(Result result, int rowNum) throws Throwable {
        User user = new User();
        String name = Bytes.toString(result.getValue(COLUMNFAMILY1,NAME));
        String info = Bytes.toString(result.getValue(COLUMNFAMILY2,INFO));
        return user.setName(name).setInfo(info);
    }
}
