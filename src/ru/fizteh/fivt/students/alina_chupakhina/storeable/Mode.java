package ru.fizteh.fivt.students.alina_chupakhina.storeable;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.Scanner;

public class Mode {
    public PrintStream out;
    public InputStream in;
    public static final String WELCOME = "$ ";
    public Mode(InputStream is, PrintStream ps) {
        out = ps;
        in = is;
    }
    Mode() {
        out = System.out;
        in = System.in;
    }
    public void interactive() throws NullPointerException {
        out.print(WELCOME);
        Scanner sc = new Scanner(in);
        boolean flag = true;
        while (flag) {
            try {

                String[] s = sc.nextLine().trim().split(";");
                for (String command : s) {
                    Interpreter i = new Interpreter(out);
                    i.doCommand(command);
                }
            } catch (Exception e) {
                out.println(e.getMessage());
            }
            out.print(WELCOME);
            if (!out.equals(System.out)) {
                throw new NullPointerException();
            }
        }
        sc.close();
    }

    public void batch(final String[] args) throws Exception {
        String arg;
        if (args.length > 0) {
            arg = args[0];
            for (int i = 1; i != args.length; i++) {
                arg = arg + ' ' + args[i];
            }
            String[] commands = arg.trim().split(";");
            for (int i = 0; i != commands.length; i++) {
                Interpreter ir = new Interpreter(out);
                ir.doCommand(commands[i]);
            }
        }
    }
}
