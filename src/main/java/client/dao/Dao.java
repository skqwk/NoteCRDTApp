package client.dao;

import java.util.List;
import java.util.Map;

public interface Dao<T> {
    void create(T entity);
    T read(Long id);
    void update(Long id, Map<String, Object> updatedFields);
    void delete(Long id);
    List<T> readAll();
}
