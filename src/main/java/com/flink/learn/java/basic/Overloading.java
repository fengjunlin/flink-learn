package com.flink.learn.java.basic;

public class Overloading {

    // 无参数 返回值为int
    public int test(){
        System.out.println("test");
        return 1;
    }

    // 有一个参数
    public void test(int a){
        System.out.println("test " + a);
    }

    // 有两个参数和一个返回值
    public String test(int a, String s){
        System.out.println("test " + a  + " " + s);
        return a + " " + s;
    }

    public static void main(String[] args) {
        Overloading o = new Overloading();
        System.out.println(o.test());
        o.test(1);
        System.out.println(o.test(1,"test3"));
    }
}
