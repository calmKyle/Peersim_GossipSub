package peersim.GossipSub;

public class Block {
    public static int block_id_counter = 0;
    public int blockID;
    public int[][] sampleMatrix;
    public int rows;
    public int columns;

    public Block(int r, int c)
    {
        this.blockID = block_id_counter++;
        this.rows = r;
        this.columns = c;
        this.sampleMatrix = new int[rows][columns];
        intialiseDataMatrix();
    }

    public void intialiseDataMatrix()
    {
        int data = 1;
        for(int i=0;i<rows;i++)
        {
            for(int j=0;j<columns;j++)
            {
                sampleMatrix[i][j] = data++;
            }
        }
    }

    public int[] getRowData(int r) //sends the rth row
    {
        return sampleMatrix[r];
    }

    public int[] getColumnData(int c) //Sends the cth column
    {
        int column[] = new int[columns];
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
