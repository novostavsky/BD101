package com.epam.bdcc.cdp;

/**
 * Created by Volodymyr_Novostavsk on 20-Apr-17.
 * Test of Fibonacci app Producer
 */

import org.junit.Assert;
import org.junit.Test;

public class FibonacciTest {
    @Test
    public void testFibonacci() {
        //Test how Fibonnaci is calculated on few numbers: 0, 1, 10, 25
        Assert.assertEquals(0, Fibonacci.getFibonacci(0));
        Assert.assertEquals(1, Fibonacci.getFibonacci(1));
        Assert.assertEquals(55, Fibonacci.getFibonacci(10));
        Assert.assertEquals(75025, Fibonacci.getFibonacci(25));
    }
}
