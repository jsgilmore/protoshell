package com.github.jsgilmore.protoshell;

import static org.junit.Assert.*;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import backtype.storm.multilang.NoOutputException;

public class ProtoSerializerTest {
    ProtoSerializer serializer;
    BufferedReader serializerOutput;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
//        PipedInputStream pipeInput = new PipedInputStream();
//        BufferedOutputStream processIn = new BufferedOutputStream(new PipedOutputStream(pipeInput));
//        serializerOutput = new BufferedReader(new InputStreamReader(pipeInput));
//
//        serializer = new ProtoSerializer();
//        serializer.initialize(processIn, null);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testConnect() {
//        try {
//            serializer.connect(null, null);
//        } catch (Exception e) {
//            fail(e.toString());
//        }
    }

    @Test
    public void testReadShellMsg() {
      //  fail("Not yet implemented");
    }

    @Test
    public void testWriteBoltMsg() {
       // fail("Not yet implemented");
    }

    @Test
    public void testWriteSpoutMsg() {
       // fail("Not yet implemented");
    }

    @Test
    public void testWriteTaskIds() {
      //  fail("Not yet implemented");
    }

}
