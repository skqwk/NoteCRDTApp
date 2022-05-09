package client.mvc.service;

import client.mvc.dao.Dao;

import java.util.List;
import java.util.Map;

public abstract class Service<T> {
    private final Dao<T> dao;

    protected Service(Dao<T> dao) {
        this.dao = dao;
    }

    public void create(T entity) {
        dao.create(entity);
    }

    public T read(Long id) {
        return dao.read(id);
    }

    public void update(Long id, Map<String, Object> updatedFields) {
        dao.update(id, updatedFields);
    }

    public void delete(Long id) {
        dao.delete(id);
    }

    public List<T> readAll(){
        return dao.readAll();
    }

}
