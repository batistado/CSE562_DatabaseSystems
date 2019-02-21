package Models;

public class Schema {
	private Integer columnIndex;
	private String dataType;
	private String columnName;
	
	public Schema(String dataType, Integer columnIndex, String columnName) {
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

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

}
