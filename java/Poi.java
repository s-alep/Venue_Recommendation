import java.io.Serializable;

public class Poi implements Serializable
{
	private int id;
	private String name, category;
	private double latitude, longtitude;

    public Poi(int id, String name, double longtitude, double latitude, String category)
    {
    	this.id = id;
    	this.name = name;
    	this.latitude = latitude;
    	this.longtitude = longtitude;
    	this.category = category;
    }
    
    public double getLongitude() { return longtitude; }

    public void setLongitude(double longitude) { this.longtitude = longitude; }
    
    public double getLatitude(){ return latitude; }
     
    public void setLatitude(double latitude){ this.latitude = latitude; }
    
    public int getId() { return id; }

    public void setId(int id) { this.id = id; }
    
    public String getCategory() { return category; }

    public void setCategory(String category) { this.category = category; }
    
    public void setName(String name) { this.name = name; }

    public String getName() { return name; }
}
 