package client.mvc.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@Builder
@ToString
public class Record implements Serializable {
    private Long id;
    private String tag = "";
    private String title = "";
    private String content = "";
    private String author = "";
}
