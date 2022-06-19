package com.github.gilvangobbato.entity;

import java.util.Random;

public enum GenderEnum {
    M, F;

    public boolean isF() {
        return this == F;
    }

    public static GenderEnum choseGender() {
        final var i = new Random().nextInt(1, 10000);
        return (i % 2) == 0 ? F : M;
    }
}
