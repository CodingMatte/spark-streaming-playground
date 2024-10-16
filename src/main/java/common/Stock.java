package common;

import java.sql.Date;

public class Stock {
  private String company;
  private Date date;
  private double value;

  // Constructor
  public Stock(String company, Date date, double value) {
    this.company = company;
    this.date = date;
    this.value = value;
  }

  // Getters and Setters
  public String getCompany() {
    return company;
  }

  public void setCompany(String company) {
    this.company = company;
  }

  public Date getDate() {
    return date;
  }

  public void setDate(Date date) {
    this.date = date;
  }

  public double getValue() {
    return value;
  }

  public void setValue(double value) {
    this.value = value;
  }

  // toString method
  @Override
  public String toString() {
    return "Stock{" + "company='" + company + '\'' + ", date=" + date + ", value=" + value + '}';
  }
}
