package com.douglas.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author douglas
 * @create 2020-11-17 11:48
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SensorReading {
    private String id;
    private Long ts;
    private Double temp;

    @Override
    public String toString() {
        return id + ", " + ts +
                ", " + temp ;
    }
}
