package shared;

import lombok.Builder;
import lombok.ToString;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Builder
@ToString
public class Payload implements Serializable {
    public List<Map<String, Object>> messages = new ArrayList<>();
    public String timestamp;
}
