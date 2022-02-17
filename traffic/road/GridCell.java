package traffic.road;

import java.util.ArrayList;

/**
 * A rectangular cell on a grid.
 *
 */
public class GridCell {
	/**
	 * Row of the cell.
	 */
	public int row;
	/**
	 * Column of the cell.
	 */
	public int col;
    /**
     * ID of the cell is the "rowTcol"
     */
    public String id;
	/**
	 * Collection of nodes that are contained in the cell.
	 */
	public ArrayList<Node> nodes = new ArrayList<>(20);
	/**
	 * Total length of lanes on the edges that start from within the cell. This
	 * length can be used for balancing the work load between workers.
	 */
	public int laneLength;

	public synchronized void setRowAndCol(int row, int col){
		this.row = row;
		this.col = col;
	}

	public synchronized int getRow(){
		return row;
	}

	public synchronized int getCol(){
		return col;
	}
}
