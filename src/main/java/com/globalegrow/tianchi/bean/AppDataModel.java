package com.globalegrow.tianchi.bean;
import lombok.Data;

import java.io.Serializable;

@Data
public class AppDataModel implements Serializable {

    private static final long serialVersionUID = 1L;

    private String platform;

    private String country_code;

    private String event_name;

    private String event_value;

    private String event_time;

}
