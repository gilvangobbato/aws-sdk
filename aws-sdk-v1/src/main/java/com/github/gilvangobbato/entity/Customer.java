package com.github.gilvangobbato.entity;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Customer {
    public static final String ID = "id";
    public static final String NAME = "name";
    public static final String COUNTRY = "country";
    public static final String CITY = "city";
    public static final String PHONE = "phone";
    public static final String STATE = "st";
    public static final String ZIP_CODE = "zip_code";
    public static final String GENDER = "gender";
    public static final String GENDER_GSI = "GenderIndex";
    public static final String STATE_GSI = "StIndex";

    private String id;
    private String name;
    private String country;
    private String state;
    private String city;
    private String zipCode;
    private String phone;
    private GenderEnum gender;
}
