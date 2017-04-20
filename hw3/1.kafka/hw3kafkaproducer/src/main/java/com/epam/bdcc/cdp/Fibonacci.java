package com.epam.bdcc.cdp;

/**
 * Created by Volodymyr_Novostavsk on 20-Apr-17.
 */
public class Fibonacci {

    public static long getFibonacci(int n) {
        //if input is 0 or 1, return the values
        //if a number if negative, also return that value
        if (n <= 1) {
            return n;
        }
        else {
            //recursive call to calculate fibonacci num
            return getFibonacci(n-1) + getFibonacci(n-2);
        }
    }
}
