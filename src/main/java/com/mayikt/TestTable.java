package com.mayikt;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.Drop;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.mayikt.pojo.Student;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

/**
 * @description:
 * @author:chenhongyan
 * @date:2021/6/14 18:57
 */
public class TestTable {

    Session session = null;


    @Before
    public void init() {
        String host = "192.168.229.103";
        int port = 9042;
        Cluster cluster = Cluster.builder()
                .addContactPoint(host)
                .withPort(port)
                .build();
        session = cluster.connect();

    }


    @Test
    public void alterCreateTable() {

        Create create = SchemaBuilder.createTable("school", "student")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("address", DataType.text())
                .addColumn("age", DataType.cint())
                .addColumn("name", DataType.text())
                .addColumn("gender", DataType.cint())
                .addColumn("interest", DataType.set(DataType.text()))
                .addColumn("phone", DataType.list(DataType.text()))
                .addColumn("education", DataType.map(DataType.text(), DataType.text()))
                .ifNotExists();
        session.execute(create);
    }

    @Test
    public void alterTable() {
        // 新增一个字段
        SchemaStatement type = SchemaBuilder.alterTable("school", "student")
                .addColumn("email")
                .type(DataType.text());
        SchemaStatement type1 = SchemaBuilder.alterTable("school", "student")
                .addColumn("email")
                .type(DataType.varchar());
        SchemaStatement type2 = SchemaBuilder.alterTable("school", "student")
                .dropColumn("email");
        session.execute(type);
    }

    @Test
    public void removeTable() {
        Drop drop = SchemaBuilder.dropTable("school", "student").ifExists();
        session.execute(drop);
    }

    @Test
    public void insertTableByCql() {
        String cql = "insert into school.student(id,address,age,gender,name,interest,phone,education) values ()";
        session.execute(cql);
    }

    @Test
    public void insertTableByMapper() {
        Set<String> interest = new HashSet<>();
        interest.add("看书");
        interest.add("电影");
        List<String> phone = new ArrayList<>();
        Map<String, String> education = new HashMap<>();
        education.put("xinming", "a");
        Student student = new Student(1012L, "朝阳路19号", "zs", 17, 1, interest, phone, education, "127.0.0.1");
        Mapper<Student> mapper = new MappingManager(session).mapper(Student.class);
        mapper.save(student);
    }


    /**
     * 查询所有数据
     */
    @Test
    public void findAll() {
        ResultSet result = session.execute(QueryBuilder.select().from("school", "student"));
        Mapper<Student> mapper = new MappingManager(session).mapper(Student.class);
        List<Student> all = mapper.map(result).all();
        for (Student student : all) {
            System.out.println(student);
        }
    }

    @Test
    public void findById() {
        ResultSet resultSet = session.execute(QueryBuilder.select().from("school", "student").where(QueryBuilder.eq("id", 1012L)));
        Mapper<Student> mapper = new MappingManager(session).mapper(Student.class);
        Student student = mapper.map(resultSet).one();
        System.out.println(student);
    }


    @Test
    public void delete() {
        Mapper<Student> mapper = new MappingManager(session).mapper(Student.class);
        Long id = 1011L;
        mapper.delete(id);
    }

    @Test
    public void createIndex() {
        SchemaStatement schemaStatement = SchemaBuilder.createIndex("nameIndex")
                .onTable("school", "student")
                .andColumn("name");
        SchemaStatement schemaStatement1 = SchemaBuilder.createIndex("educationIndex")
                .onTable("school", "student")
                .andKeysOfColumn("education");


        session.execute(schemaStatement);

    }

    @Test
    public void dropIndex() {
        Drop nameIndex = SchemaBuilder.dropIndex("school", "nameIndex");
        session.execute(nameIndex);
    }

    /**
     * 预编译操作
     */
    @Test
    public void batchPrepare() {
        BatchStatement batchStatement = new BatchStatement();
        PreparedStatement prepare = session.prepare("insert into school.student (id,address,age,gender,name,interest,phone,education) values (?,?,?,?,?,?,?,?,?)");
        for (int i = 0; i < 10; i++) {
            BoundStatement bind = prepare.bind();
            batchStatement.add(bind);
        }
        // 把批量的内容发送到服务器
        session.execute(batchStatement);
        batchStatement.clear();


    }


}
