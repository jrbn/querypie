package arch.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import arch.utils.Consts;

public class TByte extends SimpleData {
	
	int value;

	@Override
	public int getIdDatatype() {
		return Consts.DATATYPE_TBYTE;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		value = input.readByte();
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeByte(value);
	}

	@Override
	public int bytesToStore() {
		return 1;
	}

	public int getValue() {
		return value;	
	}
	
	public void setValue(int value){
		this.value = value;
	}
	
	public String toString() {
		return Integer.toString(value);
	}
}
