/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package classes;

import java.util.Date;

/**
 *
 * @author shriroop_joshi
 */
public class CrimeRecord {
    private Date crimeDate;
    private String address;
    private int district;
    private String beat;
    private int grid;
    private String decription;
    private int NCIC_code;
    private double latitude;
    private double longitude;

    public Date getCrimeDate() {
        return crimeDate;
    }

    public void setCrimeDate(Date crimeDate) {
        this.crimeDate = crimeDate;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getDistrict() {
        return district;
    }

    public void setDistrict(int district) {
        this.district = district;
    }

    public String getBeat() {
        return beat;
    }

    public void setBeat(String beat) {
        this.beat = beat;
    }

    public int getGrid() {
        return grid;
    }

    public void setGrid(int grid) {
        this.grid = grid;
    }

    public String getDecription() {
        return decription;
    }

    public void setDecription(String decription) {
        this.decription = decription;
    }

    public int getNCIC_code() {
        return NCIC_code;
    }

    public void setNCIC_code(int NCIC_code) {
        this.NCIC_code = NCIC_code;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }
    
    
    
}
