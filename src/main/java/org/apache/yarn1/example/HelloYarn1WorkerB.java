package org.apache.yarn1.example;

public class HelloYarn1WorkerB {

    public static void main(String[] args) throws InterruptedException {
        try {
            System.out.println("Hello from worker type B");
            Thread.sleep(60 * 1000);
            System.out.println("GoodBye from worker type B");
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}
