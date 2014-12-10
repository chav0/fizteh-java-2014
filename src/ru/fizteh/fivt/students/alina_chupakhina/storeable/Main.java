package ru.fizteh.fivt.students.alina_chupakhina.storeable;

import java.io.File;

/**
 * Created by opa on 16.11.2014.
 */
public class Main {
    public static String path;
    public static MyTableProvider tp;

    public static void main(final String[] args) {
        try {
            path = System.getProperty("fizteh.db.dir");
            MyTableProviderFactory tpf = new MyTableProviderFactory();
            tp = (MyTableProvider) tpf.create(path);

            if (path == null) {
                throw new Exception("Enter directory");
            }
            File dir = new File(path);
            if (!dir.exists() || !dir.isDirectory()) {
                throw new Exception("directory not exist");
            }
            File[] children = dir.listFiles();
            for (File child : children) {
                MyTable t = new MyTable(child.getName(), path, null);
                t.load();
                MyTableProvider.tableList.put(child.getName(), t);
            }
            Mode mode = new Mode();
            if (args.length > 0) {
                mode.batch(args);
            } else {
                mode.interactive();
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }
}

