package groupByDemo;

import java.util.ArrayList;
import java.util.List;

public class ExpandHbaseCondition {
	private List<ExpandHbaseCell> sumList;
	private List<ExpandHbaseCell> groupList;
	private List<ExpandHbaseCell> countList;
	private List<ExpandHbaseCell> distinctCountList;

	/**
	 * 增加要sum的字段
	 * 
	 * @param sumCell
	 */
	public void addSum(ExpandHbaseCell sumCell) {
		if (sumList == null) {
			sumList = new ArrayList<ExpandHbaseCell>();
		}
		sumList.add(sumCell);
	}

	public void addGroup(ExpandHbaseCell groupCell) {
		if (groupList == null) {
			groupList = new ArrayList<ExpandHbaseCell>();
		}
		groupList.add(groupCell);
	}

	public void addCount(ExpandHbaseCell countCell) {
		if (countList == null) {
			countList = new ArrayList<ExpandHbaseCell>();
		}
		countList.add(countCell);
	}

	public void addDistinctCount(ExpandHbaseCell distinctCell) {
		if (distinctCountList == null) {
			distinctCountList = new ArrayList<ExpandHbaseCell>();
		}
		distinctCountList.add(distinctCell);
	}

	public List<ExpandHbaseCell> getSumList() {
		return sumList;
	}

	public void setSumList(List<ExpandHbaseCell> sumList) {
		this.sumList = sumList;
	}

	public List<ExpandHbaseCell> getGroupList() {
		return groupList;
	}

	public void setGroupList(List<ExpandHbaseCell> groupList) {
		this.groupList = groupList;
	}

	public List<ExpandHbaseCell> getCountList() {
		return countList;
	}

	public void setCountList(List<ExpandHbaseCell> countList) {
		this.countList = countList;
	}

	public List<ExpandHbaseCell> getDistinctCountList() {
		return distinctCountList;
	}

	public void setDistinctCountList(List<ExpandHbaseCell> distinctCountList) {
		this.distinctCountList = distinctCountList;
	}
}
