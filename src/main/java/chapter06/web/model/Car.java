package chapter06.web.model;

public class Car {
	private int DirectId;
	private int count;
	
	public Car(){
		
	}
	public Car(int DirectId, int count){
		this.DirectId=DirectId;
		this.count=count;
	}
	public int getDirectId() {
		return DirectId;
	}
	public void setDirectId(int DirectId) {
		this.DirectId = DirectId;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public String toString(){
		return "DirectId:"+DirectId+"count:"+count;
	}
	
}
