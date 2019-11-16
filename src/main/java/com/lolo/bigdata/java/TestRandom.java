package com.lolo.bigdata.java;

import java.util.Random;

/**
 * @Author: gordon  Email:gordon_ml@163.com
 * @Date: 11/15/2019
 * @Description:
 * @Version: 1.0
 */
public class TestRandom {

    public static void main(String[] args) {

        Random random = new Random(10);
        for (int i = 0; i < 10; i++) {
            System.out.print(random.nextInt(7) + " ");
        }
        System.out.println();

        System.out.println("****************");
        Random random1 = new Random(10);
        for (int i = 0; i < 10; i++) {
            System.out.print(random1.nextInt(7) + " ");
        }


        // static final Random randomNumberGenerator = new Random();
        //  静态，只有一份
        /*Math.random()
        Math.random()
        Math.random()*/

        /*new Random().nextDouble()
        new Random().nextDouble()
        new Random().nextDouble()*/
    }
}
