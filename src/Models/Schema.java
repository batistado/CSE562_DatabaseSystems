package Models;

import java.io.Serializable;

public class Schema implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Integer columnIndex;
	private Integer dataType;
	private String columnName;
	
	public Schema(Integer dataType, Integer columnIndex, String columnName) {
		this.columnIndex = columnIndex;
		this.dataType = dataType;
		this.columnName = columnName;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public Integer getColumnIndex() {
		return columnIndex;
	}

	public void setColumnIndex(Integer columnIndex) {
		this.columnIndex = columnIndex;
	}

	public Integer getDataType() {
		return dataType;
	}

	public void setDataType(Integer dataType) {
		this.dataType = dataType;
	}

}
