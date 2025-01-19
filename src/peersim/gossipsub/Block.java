package peersim.gossipsub;

public class Block {
    public static int block_id_counter = 0;
    public int blockID;
    public byte[][] sampleMatrix;
    public int rows;
    public int columns;

    public Block(int r, int c)
    {
        this.blockID = block_id_counter++;
        this.rows = r;
        this.columns = c;
        this.sampleMatrix = new byte[rows][columns];
        intialiseDataMatrix();
    }

    public void intialiseDataMatrix()
    {
        byte data = 1;
        for(int i=0;i<rows;i++)
        {
            for(int j=0;j<columns;j++)
            {
                sampleMatrix[i][j] = data++;
            }
        }
    }

    public byte[] getRowData(int r) //sends the rth row
    {
        return sampleMatrix[r];
    }

    public byte[] getColumnData(int c) //Sends the cth column
    {
        byte[] column = new byte[columns];
        for(int i=0;i<rows;i++){
            column[i] = sampleMatrix[i][c];
        }
        return column;
    }

    public int getSample(int r,int c)
    {
        return this.sampleMatrix[r][c];
    }
}
