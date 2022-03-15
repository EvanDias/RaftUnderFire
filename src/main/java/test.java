public class test {

        public static void main(String[] args) {
            demo();
        }
        static void demo() {
            demo1();
        }
        static void demo1() {
            demo2();
        }
        static void demo2() {
            demo3();
        }
        static void demo3() {
            Thread.dumpStack();
        }
    }

