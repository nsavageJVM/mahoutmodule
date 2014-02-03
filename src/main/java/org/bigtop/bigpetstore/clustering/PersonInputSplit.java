package org.bigtop.bigpetstore.clustering;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bigtop.bigpetstore.generator.TransactionIteratorFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ubu on 2/2/14.
 */
public class PersonInputSplit extends InputSplit implements Writable {


    public int records; public TransactionIteratorFactory.Person person;

    public PersonInputSplit() { }

    public PersonInputSplit(int records, TransactionIteratorFactory.Person person) {
        this.records = records;
        this.person = person;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return 100;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(records);
        dataOutput.writeUTF(person.name());

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        records=dataInput.readInt();
        person= TransactionIteratorFactory.Person.valueOf(dataInput.readUTF());

    }
}
