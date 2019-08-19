package com.github.lawben.disco.executables;

import com.github.lawben.disco.DistributedUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.zeromq.SocketType;
import org.zeromq.Utils;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class ZMQTest {
    public List<Socket> pushers;
    public List<ZMQ.Socket> pullers;
    ZContext zContext;

    private static String IP = "localhost";

    final int numParallelSockets;
    static long BENCHMARK_TIMEOUT = TimeUnit.SECONDS.toMillis(20);

    public ZMQTest(int numParallelSockets) {
        this.numParallelSockets = numParallelSockets;
    }

    public void setupTrial() throws IOException {
        zContext = new ZContext();

        pullers = new ArrayList<>(numParallelSockets);
        pushers = new ArrayList<>(numParallelSockets);

        setupNetworkPushRun();
//        setupNetworkPullRun();
    }

    public void tearDownTrial() {
        pushers.forEach(Socket::close);
        pullers.forEach(Socket::close);
        zContext.destroy();
    }

    private void setupNetworkPushRun() throws IOException {
        Consumer<Socket> pull = (receiver) -> {
            while (true) {
                receiver.recvStr();
//                receiver.recvStr(ZMQ.DONTWAIT);
//                Object value = DistributedUtils.bytesToObject(receiver.recv(ZMQ.DONTWAIT));
            }
        };

        for (int i = 0; i < numParallelSockets; i++) {
            final int port = Utils.findOpenPort();
            ZMQ.Socket receiver = zContext.createSocket(SocketType.PULL);
            String addrBind = "tcp://*:" + port;
            System.out.println("Starting receiver on " + addrBind);
            receiver.bind(addrBind);
            Thread pullThread = new Thread(() -> pull.accept(receiver));
            pullThread.start();

            ZMQ.Socket sender = zContext.createSocket(SocketType.PUSH);
            String addrConnect = "tcp://" + IP + ":" + port;
            System.out.println("Created sender to " + addrConnect);
            sender.connect(addrConnect);
            pushers.add(sender);
        }

    }

    private void setupNetworkPullRun() throws IOException {
        Consumer<ZMQ.Socket> push = (eventSender) -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            while (true) {
                Long value = 10L;
                eventSender.sendMore(String.valueOf(0));
                eventSender.sendMore(String.valueOf(1000));
                eventSender.send(DistributedUtils.objectToBytes(value), ZMQ.DONTWAIT);
            }
        };

        List<Thread> senders = new ArrayList<>(numParallelSockets);
        for (int i = 0; i < numParallelSockets; i++) {
            int port = Utils.findOpenPort();

            ZMQ.Socket sender = zContext.createSocket(SocketType.PUSH);
            sender.connect("tcp://" + IP + ":" + port);
            Thread sendThread = new Thread(() -> push.accept(sender));
            senders.add(sendThread);

            ZMQ.Socket receiver = zContext.createSocket(SocketType.PULL);
            receiver.bind("tcp://*:" + port);
            pullers.add(receiver);
        }

        senders.forEach(Thread::start);
    }

    public static void benchmarkZMQPush(BMInfo info) {
        Socket sender = info.socket;
        while (!info.isStop()) {
            Long value = 10L;
//            DON'T USE SENDMORE
            sender.send(String.valueOf(0), ZMQ.DONTWAIT);
//            sender.send(String.valueOf(1000), ZMQ.DONTWAIT);
//            sender.send(DistributedUtils.objectToBytes(value), ZMQ.DONTWAIT);
            info.counter.getAndIncrement();
        }

        System.out.println("ENDING PUSH BENCHMARK");
    }

    public static void benchmarkZMQPull(BMInfo info) {
        Socket receiver = info.socket;
        while (!info.isStop()) {
            String id = receiver.recvStr();
            String window = receiver.recvStr(ZMQ.DONTWAIT);
            Object value = DistributedUtils.bytesToObject(receiver.recv(ZMQ.DONTWAIT));
            info.counter.getAndIncrement();
        }

        System.out.println("ENDING PULL BENCHMARK");
    }

    public static void benchmarkZMQ(List<Socket> sockets, Consumer<BMInfo> benchmarkFn) throws Exception {
        AtomicLong counter = new AtomicLong();

        List<Thread> threads = new ArrayList<>();
        List<BMInfo> infos = new ArrayList<>();
        for (final Socket s : sockets) {
            BMInfo info = new BMInfo(s, counter);
            threads.add(new Thread(() -> benchmarkFn.accept(info)));
            infos.add(info);
        }

        runZMQ(threads, infos);
    }


    public static void runZMQ(List<Thread> threads, List<BMInfo> infos) throws Exception {
        final long startTime = System.currentTimeMillis();

        threads.forEach(Thread::start);
        while (true) {
            if (!(System.currentTimeMillis() - startTime < BENCHMARK_TIMEOUT))
                break;
            // Do nothing, wait
        }
        final long endTime = System.currentTimeMillis();
        infos.forEach(BMInfo::stop);
        final long duration = endTime - startTime;
        final long count = infos.get(0).counter.get();

        for (Thread t : threads) {
            t.join();
        }



        System.out.println();
        System.out.println("BENCHMARK RESULTS");
        System.out.println("-----------------");
        System.out.println("DURATION:      " + duration + " ms");
        System.out.println("TOTAL COUNTER: " + count);
        System.out.println("EVENTS/S:      " + (count / duration * 1000));
        System.out.println();
    }

    public static void main(String[] args) throws Exception {
        final int numParallelSockets = 16;

        System.out.println("Press enter to start");
        System.in.read();

        System.out.println("RUNNING WITH " + numParallelSockets + " PARALLEL SOCKETS.");
        ZMQTest test = new ZMQTest(numParallelSockets);
        test.setupTrial();

        System.out.println("STARTING PUSH BENCHMARK");
        List<Socket> pushers = test.pushers;
        benchmarkZMQ(pushers, ZMQTest::benchmarkZMQPush);
        System.out.println("FINISHED PUSH BENCHMARK");


        System.out.println("Press enter to end");
        System.in.read();

//        System.out.println("STARTING PULL BENCHMARK");
//        List<Socket> pullers = test.pullers;
//        benchmarkZMQ(pullers, ZMQTest::benchmarkZMQPull);
//        System.out.println("FINISHED PULL BENCHMARK");

        test.tearDownTrial();
    }


    static class BMInfo {
        public final Socket socket;
        public final AtomicLong counter;
        public boolean stop = false;

        public BMInfo(Socket socket, AtomicLong counter) {
            this.socket = socket;
            this.counter = counter;
        }

        public void stop() {
            this.stop = true;
        }

        public boolean isStop() {
            return this.stop;
        }
    }

}
