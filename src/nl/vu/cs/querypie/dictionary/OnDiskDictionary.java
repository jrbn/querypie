package nl.vu.cs.querypie.dictionary;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OnDiskDictionary {

    static Logger log = LoggerFactory.getLogger(OnDiskDictionary.class);

    static Map<Long, String> common = new HashMap<Long, String>();

    static long[] hash;
    static long[] offset;
    static int[] length;
    RandomAccessFile data;

    public void load(String sorted_index, String data_file) throws IOException {
        // Load the dictionary and serve requests

        long start = System.currentTimeMillis();

        File f = new File(sorted_index);
        long count = f.length() / (8 + 8 + 4);

        log.info("  Loading sorted index (" + count + ")...");

        DataInputStream din = new DataInputStream(new BufferedInputStream(
                new FileInputStream(f)));

        hash = new long[(int) count];
        offset = new long[(int) count];
        length = new int[(int) count];

        for (int i = 0; i < count; i++) {

            hash[i] = din.readLong();
            offset[i] = din.readLong();
            length[i] = din.readInt();

            if (i % 1000000 == 0) {
                log.info("Read " + i);
            }
        }

        din.close();

        long end = System.currentTimeMillis();
        log.info("  Done in " + (end - start));

        data = new RandomAccessFile(new File(data_file), "r");
    }

    public String[] getText(long... resources) {
        String[] output = new String[resources.length];

        for (int i = 0; i < resources.length; ++i) {
            long tmp = resources[i];

            if (common.containsKey(tmp)) {
                String s = common.get(tmp);
                if (s != null) {
                    output[i] = s;
                }
            } else {
                int index = Arrays.binarySearch(hash, tmp);
                if (index > 0) {
                    // found it!

                    try {
                        long off = offset[index];
                        int len = length[index];

                        byte[] b = new byte[len];

                        data.seek(off);
                        data.read(b, 0, len);

                        output[i] = new String(b);
                    } catch (Exception e) {
                        log.error("Failed to read index " + index, e);
                    }
                }
            }
        }
        return output;
    }

    public static void main(String[] args) throws Exception {

        // Load the dictionary and serve requests

        long start = System.currentTimeMillis();

        /*
         * for (Entry<String, Long> entry : TriplesUtils.getInstance()
         * .getPreloadedURIs().entrySet()) { common.put(entry.getValue(),
         * entry.getKey().substring(1, entry.getKey().length() - 1)); }
         */

        long end = System.currentTimeMillis();

        log.info("  Done in " + (end - start));

        start = end;

        File f = new File(args[0]);
        long count = f.length() / (8 + 8 + 4);

        log.info("  Loading sorted index (" + count + ")...");

        DataInputStream din = new DataInputStream(new BufferedInputStream(
                new FileInputStream(f)));

        hash = new long[(int) count];
        offset = new long[(int) count];
        length = new int[(int) count];

        for (int i = 0; i < count; i++) {

            hash[i] = din.readLong();
            offset[i] = din.readLong();
            length[i] = din.readInt();

            if (i % 1000000 == 0) {
                log.info("Read " + i);
            }
        }

        din.close();

        end = System.currentTimeMillis();

        log.info("  Done in " + (end - start));

        File rf = new File(args[1]);

        RandomAccessFile data = new RandomAccessFile(rf, "r");

        ServerSocket socket = null;
        // Open the HTTP socket
        try {
            socket = new ServerSocket(4444);
            while (true) {
                Socket client = socket.accept();
                try {
                    PrintWriter out = new PrintWriter(client.getOutputStream(),
                            true);
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(client.getInputStream()));
                    String inputLine;

                    while ((inputLine = in.readLine()) != null) {
                        // Contains three numbers to process
                        String[] resources = inputLine.split(" ");
                        // log.info("Request to process " + inputLine);

                        for (int i = 0; i < resources.length; ++i) {

                            long tmp = Long.valueOf(resources[i]);

                            if (common.containsKey(tmp)) {
                                String s = common.get(tmp);

                                if (s != null) {
                                    resources[i] = s;
                                }

                            } else {

                                int index = Arrays.binarySearch(hash, tmp);

                                if (index > 0) {
                                    // found it!

                                    try {
                                        long off = offset[index];
                                        int len = length[index];

                                        byte[] b = new byte[len];

                                        data.seek(off);
                                        data.read(b, 0, len);

                                        resources[i] = new String(b);
                                    } catch (Exception e) {
                                        log.error("Failed to read index "
                                                + index, e);
                                    }
                                }
                            }
                        }

                        for (String resource : resources) {
                            out.println(resource);
                        }
                    }
                } catch (Exception e) {
                    log.error("Something went wrong ...", e);
                }
            }
        } catch (Exception e) {
            log.error("Error in creating the socket", e);
        } finally {
            if (socket != null) {
                socket.close();
            }
            data.close();
        }

    }
}
