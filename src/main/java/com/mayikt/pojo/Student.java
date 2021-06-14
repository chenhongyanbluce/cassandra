package com.mayikt.pojo;

import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @description:
 * @author:chenhongyan
 * @date:2021/6/14 20:37
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Table(keyspace = "school",name = "student")
public class Student {

    @PartitionKey
    private Long id;
    private String address;
    private String name;
    private Integer age;
    private Integer gender;
    private Set<String> interest;
    private List<String> phone;
    private Map<String,String> education;
    private String email;
}
