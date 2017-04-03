package com.epam.cdp;

/**
 * Get top N iPinYouIDMap ids from files.
 */

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

public class Top100iPinYouIDMap {
//    private String fileToRead;
    private String fileToWrite;
    private HashMap idMap;
    private int N;

    public Top100iPinYouIDMap(String fileToWrite, int N){
//        this.fileToRead = fileToRead;
        this.fileToWrite = fileToWrite;
        this.idMap = new HashMap< String, Integer >();
        this.N = N;
    }

    private BufferedReader getReader(String file) throws IOException{
        Path pt=new Path(file);
        FileSystem fs = FileSystem.get(new Configuration());
        return new BufferedReader(new InputStreamReader(fs.open(pt)));
    }

    private BufferedWriter getWriter() throws IOException{
        Path pt=new Path(this.fileToWrite);
        FileSystem fs = FileSystem.get(new Configuration());
        return new BufferedWriter(new OutputStreamWriter(fs.create(pt)));
    }

    private void addIdtoMap(String id){
        if(! this.idMap.containsKey(id)){
            this.idMap.put(id, 1);
        } else{
            this.idMap.replace(id,new Integer(((Integer) this.idMap.get(id)) +1));
        }
    }

    private List<String> getTopN() {
        final HashMap<String, Integer> map = this.idMap;
        Set<String> set = map.keySet();
        List<String> keys = new ArrayList<String>(set);

        Collections.sort(keys, new Comparator<String>() {
            @Override
            public int compare(String s1, String s2) {
                return Integer.compare(map.get(s2), map.get(s1)); //reverse order
            }
        });
        return keys.subList(0, this.N);
    }


    protected void readFile(String file)throws IOException{
        BufferedReader br= this.getReader(file);
        String line;
        line=br.readLine();

        try{
            while (line != null){
                String[] strArray = line.split("\t");
                this.addIdtoMap(strArray[2]);
                line=br.readLine();
            }
        } finally {
            if (br != null) br.close();
        }
    }

    protected void writeResults()throws IOException{
        Writer writer = this.getWriter();
        Iterator<String> i = this.getTopN().iterator();

        try {
            while (i.hasNext()) {
                writer.write(i.next() + "\t");
            }
        } finally {
            if (writer != null) writer.close();
        }
    }

    protected static String[] getFilesList(String dir) throws IOException{
        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] status = fs.listStatus(new Path(dir));

        String[] filesList = new String[status.length];
        for(int i = 0; i<status.length; i++){
            filesList[i] = status[i].getPath().toString();
        }
        return filesList;
    }

    public static void main(String[] args) {

        Top100iPinYouIDMap inst = new Top100iPinYouIDMap(args[1], Integer.parseInt(args[2]));

        try {
            String[] filesList = Top100iPinYouIDMap.getFilesList(args[0]);

            for(int i=0; i<filesList.length; i++){
                inst.readFile(filesList[i]);
            }
            inst.writeResults();

        } catch(IOException e){
            e.printStackTrace();
        }
    }
}
