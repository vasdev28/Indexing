import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;

import com.opencsv.CSVReader;

class MyDatabase {
	static final String datadb_path = "data.db";
	static final String idndx_path = "id.ndx";
	static final String statendx_path = "state.ndx";
	static final String lnamendx_path = "lname.ndx";
	static final String csv_path = "us-500.csv";
	
	static HashMap<Integer,Long> hmId = new HashMap<Integer,Long>();
	static HashMap<String,ArrayList<Long>> hmState = new HashMap<String,ArrayList<Long>>();
	static HashMap<String,ArrayList<Long>> hmLname = new HashMap<String,ArrayList<Long>>();
		
	private static void select(String field,String key) {
		try {
			RandomAccessFile readfile = new RandomAccessFile(datadb_path, "r");
			if (field.equalsIgnoreCase("id")) {
				System.out.println("SELECT * FROM MyDatabase WHERE "+field+"="+key+";");
				Long recordOffset = hmId.get(Integer.parseInt(key));
				readfile.seek(recordOffset);
				ArrayList<String> row = getFields(recordOffset);
				for(String attr:row) {
					System.out.print(attr+"\t|");
				}
				System.out.println("");
			} else {
				System.out.println("SELECT * FROM MyDatabase WHERE "+field+"='"+key+"';");
				ArrayList<Long> recordOffset_list;
				if (field.equalsIgnoreCase("state")) {
					recordOffset_list = hmState.get(key);
				} else {
					recordOffset_list = hmLname.get(key);
				}
				for (Long recordOffset:recordOffset_list) {
					ArrayList<String> row = getFields(recordOffset);
					for(String attr:row) {
						System.out.print(attr+"\t|");
					}
					System.out.println("");
				}
				if(recordOffset_list.size()<1) {
					System.out.println("Record doesn't exist");
				}
			}
			readfile.close();
		} catch (IOException e) {
			System.out.println("IO Error");
		} catch (NullPointerException e1) {
			System.out.println("Record doesn't exist");
		}
	}
	
	private static void insert(String id,String fname,String lname,String cname, String add,String city,
				String cnty,String state,String zip,String phone1,String phone2,String email,String web,int method) {
		if(method!=0) {
			System.out.println("INSERT INTO MyDatabase VALUES ('"+id+"','"+fname+"','"+lname+"','"+cname+"','"+add+"','"+
				city+"','"+cnty+"','"+state+"','"+zip+"','"+phone1+"','"+phone2+"','"+email+"','"+web+"');");
		}
		if(hmId.containsKey(Integer.parseInt(id))) {
			System.out.println("A Record with id = "+id+" already exists. Primary Key Id must be unique");
			return;
		}
		try {
			RandomAccessFile dbFile = new RandomAccessFile(datadb_path,"rw");
			long fp = dbFile.length();
			dbFile.seek(fp);
			dbFile.writeInt(Integer.parseInt(id));
			dbFile.writeBytes("\t"+fname+"\t"+lname+"\t"+cname+"\t"+add+"\t"+city+"\t"+cnty+"\t"+state+"\t");
			dbFile.writeBytes(zip);
			dbFile.writeBytes("\t"+phone1+"\t"+phone2+"\t"+email+"\t"+web+";");
			dbFile.close();
			updateKeyHashMap(Integer.parseInt(id),fp,1);
			updateNonKeyHashMap(lname,fp,hmLname,1);
			updateNonKeyHashMap(state,fp,hmState,1);
			if(method!=0) {System.out.println("Success");}
		} catch (IOException e) {
			System.out.println("IO Exception when trying to write");
			e.printStackTrace();
		}
	}

	private static void insert(String id,String fname,String lname,String cname, String add,String city,
			String cnty,String state,int zip,String phone1,String phone2,String email,String web) {
		//OverLoaded Method
		insert(id, fname, lname, cname, add, city, cnty, state, Integer.toString(zip), phone1, phone2, email, web,1);
    }
	
	private static void insert(String id,String fname,String lname,String cname, String add,String city,
			String cnty,String state,String zip,String phone1,String phone2,String email,String web) {
		//OverLoaded Method
		insert(id, fname, lname, cname, add, city, cnty, state, zip, phone1, phone2, email, web,1);
    }
	
	private static void delete(int id,int method) {
		if (method!=0) {
			System.out.println("DELETE FROM MyDatabase WHERE id="+id+";");	
		}
		if (hmId.containsKey(id)) {
			try {
				RandomAccessFile dbFile = new RandomAccessFile(datadb_path,"rw");
				Long recPtr = hmId.get(id);
				dbFile.seek(recPtr);
				ArrayList<String> row = getFields(recPtr);
				String lName = row.get(2);
				String state = row.get(7);
				byte rByte = dbFile.readByte();
				while(rByte!=';') {
					dbFile.seek(dbFile.getFilePointer()-1);
					dbFile.writeByte(0);
					rByte = dbFile.readByte();
				}
				dbFile.close();
				updateKeyHashMap(id, recPtr, 0);
				updateNonKeyHashMap(lName,recPtr,hmLname,0);
				updateNonKeyHashMap(state,recPtr,hmState,0);
				if(method!=0) {System.out.println("Success");}
			} catch (IOException e) {
				System.out.println("IO Exception in Delete");
			}
		} else {
			System.out.println("Key doesn't exist");
		}		
	}
	
	private static void delete(int id) {
		delete(id,1);
	}
	
	private static void modify(int id, String field, String new_value) {
		System.out.println("UPDATE MyDatabase SET "+field+" = '"+new_value+"' WHERE id="+id+";");
		Long recordOffset = hmId.get(id);
		if(recordOffset!=null) {
			ArrayList<String> row = getFields(recordOffset);
			delete(id,0);
			row.remove(field2Ind(field));
			row.add(field2Ind(field),new_value);
			insert(row.get(0),row.get(1),row.get(2),row.get(3),row.get(4),row.get(5),
					row.get(6),row.get(7),row.get(8),row.get(9),row.get(10),row.get(11),row.get(12),0);
			System.out.println("Success");
		} else {
			System.out.println("You are trying to modify a non-existent record");
		}
	}

	private static void count() {
		System.out.println("SELECT COUNT(*) FROM MyDatabase");
		try {
			RandomAccessFile dbFile = new RandomAccessFile(datadb_path,"r");
			System.out.println("Total number of record in the database = "+hmId.size());
			dbFile.close();
		} catch (IOException e) {
			System.out.println("Error accessing the db file");
		}
	}

	private static ArrayList<String> getFields(Long fp) {
		try {
			RandomAccessFile dbFile = new RandomAccessFile(datadb_path,"r");
			dbFile.seek(fp);
			ArrayList<String> row = new ArrayList<String>();
			row.add(Integer.toString(dbFile.readInt()));
			dbFile.readByte();
			String field="";
			char c;
			while((c= (char) dbFile.readByte())!=';') {
				if (c=='\t') {
					row.add(field);
					field = "";
				} else {
					field = field + c;
				}
			}
			row.add(field);
			dbFile.close();
			return row;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	private static int field2Ind(String field) {
		ArrayList<String> str=new ArrayList<String>();
		str.add("id");
		str.add("first_name");
		str.add("last_name");
		str.add("company_name");
		str.add("address");
		str.add("city");
		str.add("county");
		str.add("state");
		str.add("zip");
		str.add("phone1");
		str.add("phone2");
		str.add("email");
		str.add("web");
		return str.indexOf(field);	
	}
		
	private static void updateKeyHashMap(int id,long fp, int add) {
		if(add==1) {
			hmId.put(id, fp);
		} else {
			hmId.remove(id);
		}
	}
	
	private static void updateNonKeyHashMap(String key,long value,HashMap<String,ArrayList<Long>> hm, int add) {
		if(hm.containsKey(key)) {
			if(add==1) {
				ArrayList<Long> local = hm.get(key);
				local.add(value);
				hm.put(key, local);
			} else {
				ArrayList<Long> fd_list_old = hm.get(key);
				ArrayList<Long> fd_list_new = new ArrayList<Long>();
				for(Long fd:fd_list_old) {
					if(fd !=value) {
						fd_list_new.add(fd);
					}
				}
				hm.put(key, fd_list_new);
			}
		} else {
			if(add==1) {
				ArrayList<Long> local = new ArrayList<Long>();
				local.add(value);
				hm.put(key, local);
			} else {
				System.out.println("Hash Entry doesn't exist");
			}
		}
	}
	
	private static void writeKeyHashMapToFile(HashMap<Integer,Long> hm, String file_path) {
		try {
			FileOutputStream fileOut = new FileOutputStream(file_path);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(hm);
			out.close();
			fileOut.close();
		} catch (IOException e) {
			System.out.println("Error in writing hash map to file");
		}
	}
	
	private static void writeNonKeyHashMapToFile(HashMap<String,ArrayList<Long>> hm, String file_path) {
		try {
			FileOutputStream fileOut = new FileOutputStream(file_path);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(hm);
			out.close();
			fileOut.close();
		} catch (IOException e) {
			System.out.println("Error in writing hash map to file");
		}
	}
	
	@SuppressWarnings("unchecked")
	private static void readHashMapFromFile() {
		try {
			FileInputStream fileIn1 = new FileInputStream(idndx_path);
			FileInputStream fileIn2 = new FileInputStream(statendx_path);
			FileInputStream fileIn3 = new FileInputStream(lnamendx_path);
			FileInputStream fileIn4 = new FileInputStream(datadb_path);
			ObjectInputStream in1 = new ObjectInputStream(fileIn1);
			ObjectInputStream in2 = new ObjectInputStream(fileIn2);
			ObjectInputStream in3 = new ObjectInputStream(fileIn3);

			hmId = (HashMap<Integer, Long>) in1.readObject();
			hmState = (HashMap<String, ArrayList<Long>>) in2.readObject();
			hmLname = (HashMap<String, ArrayList<Long>>) in3.readObject();
			
			in1.close();
			in2.close();
			in3.close();
			fileIn1.close();
			fileIn2.close();			
			fileIn3.close();
			fileIn4.close();
			
		} catch (IOException | ClassNotFoundException e) {
			System.out.println("Starting Fresh");
			clearFiles();
		}
	}
			
	private static void loadDataFromCSV() {
		try {
			CSVReader csvReader = new CSVReader(new FileReader(csv_path));
			csvReader.readNext();
			String[] line = csvReader.readNext();
			while(line!=null) {
				String parsed_line[] = line;
				if(parsed_line.length!=13) {
					System.out.println("Ignoring line as not sufficient records in line : "+line);
				} else {
					String id = parsed_line[0];
					String fname = parsed_line[1];
					String lname = parsed_line[2];
					String cname = parsed_line[3];
					String add = parsed_line[4];
					String city = parsed_line[5];
					String cnty = parsed_line[6];
					String state = parsed_line[7];
					String zip = parsed_line[8];
					String phone1 = parsed_line[9];
					String phone2 = parsed_line[10];
					String email = parsed_line[11];
					String web = parsed_line[12];
					insert(id,fname,lname,cname,add,city,cnty,state,zip,phone1,phone2,email,web);
				}
				line = csvReader.readNext();
			}
			csvReader.close();
		} catch(IOException e1) {
			System.out.println("CSV File Missing. Working with the existing data.db & index files");
		}
	}

	private static void clearFiles() {
		try {
			File file1 = new File(datadb_path);
			file1.delete();
			file1.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			File file2 = new File(idndx_path);
			file2.delete();
			file2.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			File file3 = new File(lnamendx_path);
			file3.delete();
			file3.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			File file4 = new File(statendx_path);
			file4.delete();
			file4.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	public static void main(String args[]) {	
		readHashMapFromFile();
		loadDataFromCSV();
		select("state","TX");
		select("id","2");
		modify(2,"address","2200 waterview pkwy");
		select("id","2");
		select("id","3");
		delete(3);
		select("id","3");
		count();
		insert("504","Mitsue","Tollner","Morlong Associates","7 Eads St","Chicago","Cook","IL","60632","773-573-6914","773-924-8565","mitsue_tollner@yahoo.com","http://www.morlongassociates.com");
		insert("504","Mitsue","Tollner","Morlong Associates","7 Eads St","Chicago","Cook","IL",60632,"773-573-6914","773-924-8565","mitsue_tollner@yahoo.com","http://www.morlongassociates.com");
		insert("1572","Jennifer","Fallick","Nagle, Daniel J Esq","44 58th St","Wheeling","Cook","IL",60090,"847-979-9545","847-800-3054","jfallick@yahoo.com","http://www.nagledanieljesq.com");
		count();
		select("id","572");
		writeKeyHashMapToFile(hmId,idndx_path);
		writeNonKeyHashMapToFile(hmLname,lnamendx_path);
		writeNonKeyHashMapToFile(hmState,statendx_path);
		System.out.println("Done");
	}
}