import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoMessage;

@ProtoMessage(name = "user")
public class User {

    @ProtoField(number = 1, required = true)
    private String name;

    @ProtoField(number = 2, required = true)
    private int age;

    public User(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public User() {
        this.name = "";
        this.age = -1;
    }

}
