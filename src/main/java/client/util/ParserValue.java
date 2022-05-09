package client.util;

import java.util.function.Function;

public enum ParserValue {
    STRING(Object::toString),
    LONG(Long::parseLong);

    public final Function<String, Object> parse;

    ParserValue(Function<String, Object> parse) {
        this.parse = parse;

    }
}
