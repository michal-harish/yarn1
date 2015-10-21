package io.amient.yarn1.example;

public class HelloYarn1WorkerA {

    public static void main(String[] args) throws InterruptedException {
        try {
            System.out.println("Hello from worker type A");
            Thread.sleep(30 * 1000);
            System.out.println("Goodbyefrom worker type A");
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}
