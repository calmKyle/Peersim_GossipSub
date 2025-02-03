/*
package peersim.gossipsub;
// This class is not in use currently because it was not evenly distributing the rows and cols to the topics
// You could use DHT or a better hash function to distribute the rows and cols evenly. but have justed skipped that over here

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class RowColumnDistributor {
//    private static final int NUMBER_OF_ROWS = 512;
//    private static final int NUMBER_OF_COLUMNS = 512;

    public int numberOfRows = 0;
    public int numberOfColumns =0;
    public RowColumnDistributor(int rows,int col)//Taking no. of rows & cols just in case you want to make it dynamic
    {
        this.numberOfRows = rows;
        this.numberOfColumns = col;
    }

    public int fNode(BigInteger nodeID,int epoch, int slot) throws NoSuchAlgorithmException { //This function allocates one row/col per node
        String str = nodeID.toString()+epoch+slot;
        // Create a MessageDigest instance for SHA-256
        MessageDigest digest = MessageDigest.getInstance("SHA-256");

        // Perform the hash computation
        byte[] encodedhash = digest.digest(str.getBytes());

        BigInteger hashValue = new BigInteger(1,encodedhash);

        int totalAllocations = numberOfRows+numberOfColumns;
//        int allocation = hashValue.mod(BigInteger.valueOf(totalAllocations)).intValue();  //Taking the mod of the hash value get the num bt. 0 and totalAllocations

        return hashValue.mod(BigInteger.valueOf(totalAllocations)).intValue();
    }
}

//************NOTE******************8
//The fNode function does not give unique rows/cols to the nodes
*/
