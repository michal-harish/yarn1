package net.imagini.yarn1.example;

public class HelloYarn1WorkerB {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Hello from worker type B");
        Thread.sleep(10000);
        System.out.println("GoodBye from worker type B");
    }

}
