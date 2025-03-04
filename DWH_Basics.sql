-- Databricks notebook source
-- MAGIC %md
-- MAGIC Incremental Data Loading

-- COMMAND ----------

create database Sales_n;

-- COMMAND ----------

DROP Database Sales;

-- COMMAND ----------

-- MAGIC %fs rm dbfs:/user/hive/warehouse/Sales.db/Orders

-- COMMAND ----------

CREATE TABLE Sales_n.Orders (
  OrderID INT,
  OrderDate DATE,
  CustomerID INT,
  CustomerName STRING,
  CustomerEmail STRING,
  ProductID INT,
  ProductName STRING,
  ProductCategory STRING,
  RegionID INT,
  RegionName STRING,
  Country STRING,
  Quantity INT,
  UnitPrice DECIMAL(10, 2),
  TotalAmount DECIMAL(10, 2)
);

-- COMMAND ----------

Insert into Sales_n.Orders(OrderID, OrderDate, customerID, CustomerName, CustomerEmail, ProductID,
ProductName, ProductCategory, RegionID, RegionName, Country, Quantity, UnitPrice, TotalAmount)
VALUES
(1,'2024-02-01',101,'Alice Johnson','alice@example.com',201,'Laptop','Electronics',301,'North America','USA',2,800.00,1600.00),
(2,'2024-02-02',102,'Bob Smith','bob@example.com',202,'Smartphone','Electronics',302,'Europe','Germany',1,500.00,500.00),
(3,'2024-02-03',103,'Charlie Brown','charlie@example.com',203,'Tablet','Electronics',303,' Asia','India',3,300.00,900.00),
(4,'2024-02-04',101,'Alice Johnson','alice@example.com',204,'Headphones','Accessories',301,'North America','USA',1,150.00,150.00),
(5,'2024-02-05',104,'David Lee','david@example.com',205,'Gaming Console','Electronics',302,'Europe','France',1,400.00,400.00),
(6,'2024-02-06',102,'Bob Smith','bob@example.com',206,'Smartphone','Electronics',302,'Asia','China',2,200.00,400.00),
(7,'2024-02-07',105,'Eve Adams','eve@example.com',201,'Laptop','Electronics',301,'North America','Canada',1,800.00,800.00),
(8,'2024-02-08',106,'Frank Miller','frank@example.com',207,'Monitor','Accessories',302,'Europe','Italy',2,250.00,500.00),
(9,'2024-02-09',107,'Grace White','grace@example.com',208,'Keyboard','Accessories',303,'Asia','Japan',3,100.00,300.00),
(10,'2024-02-10',104,'David Lee','david@example.com',209,'Mouse','Accessories',301,'North America','USA',1,50.00,50.00);

-- COMMAND ----------

 select * from Sales_n.orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### New Records

-- COMMAND ----------

Insert into Sales.Orders(OrderID, OrderDate, customerID, CustomerName, CustomerEmail, ProductID,
ProductName, ProductCategory, RegionID, RegionName, Country, Quantity, UnitPrice, TotalAmount)
VALUES
(11,'2024-02-11',108,'Hannah Green','hannah@example.com',210,'Wireless Earbuds','Accessories',302,'Europe','Spain',2,120.00,120.00),
(12,'2024-02-12',109,'Ian Bob','ian@example.com',201,'Laptop','Electronics',303,'Asia','India',1,800.00,800.00),
(13,'2024-02-13',105,'Eve Adams','charlie@example.com',202,'Smartphone','Electronics',301,' North America','Canada',1,500.00,500.00),
(14,'2024-02-14',110,'Jack Wilson','jack@example.com',211,'External Hard Disk','Accessories',302,'Europe','UK',2,150.00,300.00),
(15,'2024-02-15',101,'Alice Johnson','alice@example.com',203,'tablet','Electronics',301,'North America','USA',1,300.00,300.00);

-- COMMAND ----------

select * from Sales_n.orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #DATA WAREHOUSING
-- MAGIC (Also same way to create DB but here's the place we do ETL.)

-- COMMAND ----------

create database SalesDWH_n

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Staging

-- COMMAND ----------

-- without Where condition it will pull all the records as such.
create or replace table SalesDWH_n.stg_table
AS
SELECT * FROM Sales_n.Orders
--where OrderDate>'2024-02-10'

-- COMMAND ----------

select * from SalesDWH_n.stg_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Transform

-- COMMAND ----------

create or replace view SalesDWH_n.trans_sales
as 
select * from SalesDWH_n.stg_table where Quantity is not null

-- COMMAND ----------

select * from SalesDWH_n.trans_sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CORE LAYER FOR DIMENSION TABLE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DIMSENSIONS TABLE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### DimCustomerTable

-- COMMAND ----------

create or replace table SalesDWH_n.DimCustomers
(
  CustomerID INT,
  DimCustKey INT,
  CustomerName String,
  CustomerEmail String 
)

-- COMMAND ----------

/*select t.*, row_number() over(order by CustomerID) as DimCustKey from 
(
  Select distinct(CUSTOMERID),
  CustomerName,
  CustomerEmail
  from salesdwh_n.trans_sales
) t;*/

-- COMMAND ----------

create or replace view salesdwh_n.view_DimCustomers
as 
select t.*, row_number() over(order by t.CustomerID) as DimCustKey from 
(
  Select distinct(CUSTOMERID) CustomerID,
  CustomerName,
  CustomerEmail
  from salesdwh_n.trans_sales
) t;

-- COMMAND ----------

insert into SalesDWH_n.DimCustomers
(CustomerID, DimCustKey, CustomerName, CustomerEmail)
SELECT 
  CustomerID, 
  DimCustKey, 
  CustomerName, 
  CustomerEmail 
FROM salesdwh_n.view_DimCustomers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### ProductDim

-- COMMAND ----------

create or replace table SalesDWH_n.Prod_Dim
(
  ProductID Int,
  ProductDimKey Int,
  ProductName String,
  ProductCategory String
)

-- COMMAND ----------

create or replace view salesdwh_n.view_Product
as select U.*,row_number() over(order by ProductID) ProductDimKey from
(
  select distinct(ProductID) ProductID,
  ProductName, ProductCategory
  from salesdwh_n.trans_sales
)U

-- COMMAND ----------

insert into salesdwh_n.Prod_Dim
(ProductID,ProductDimKey,ProductName,ProductCategory)
select ProductID,ProductDimKey,ProductName,ProductCategory
from salesdwh_n.view_Product;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### RegionTable

-- COMMAND ----------

create or replace table salesDwh_n.RegionT
(
  RegionID int,
  RegionKey int,
  RegionName string,
  Country string
)

-- COMMAND ----------

create or replace view salesdwh_n.view_Region
as select t.*,row_number() over(order by regionID) RegionKey from
(
  select distinct(regionID), RegionName, Country
  from salesdwh_n.trans_sales
)t

-- COMMAND ----------

insert into salesdwh_n.RegionT
(regionID, RegionName, Country,RegionKey)
select regionID, RegionName, Country,RegionKey from salesdwh_n.view_Region

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Date Table

-- COMMAND ----------

create or replace table salesdwh_n.DateT
(
  OrderDate date,
  DatID int
)

-- COMMAND ----------

create or replace view salesdwh_n.view_Date
as select t.*,row_number() over(order by OrderDate) DatID from
(select
distinct(OrderDate) OrderDate
FROM salesdwh_n.trans_sales
)t

-- COMMAND ----------

insert into salesdwh_n.DateT
select * from salesdwh_n.view_Date

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #FACT Table

-- COMMAND ----------

create or replace table salesdwh_n.Fact_Table
(
OrderID int, 
Quantity int ,
UnitPrice decimal,
TotalAmount decimal,
DimCustKey int,
ProductDimKey int,
RegionKey int,
DatID int
)

-- COMMAND ----------

create or replace view salesdwh_n.view_Fact
as
 select
F.OrderID , 
F.Quantity ,
F.UnitPrice ,
F.TotalAmount,
C.DimCustKey ,
p.ProductDimKey,
r.RegionKey,
d.datid
from salesdwh_n.trans_sales f 
left join salesdwh_n.DimCustomers c
on f.CustomerID=c.customerID
left join salesdwh_n.prod_dim p
on f.productID=p.productID
left join salesdwh_n.datet d
on f.orderdate=d.orderdate 
left join salesdwh_n.RegionT r

on f.country=r.country and f.regionID=r.regionId and f.RegionName=r.RegionName

-- COMMAND ----------

insert into salesdwh_n.fact_table
select * from salesdwh_n.view_Fact

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Core Layer

-- COMMAND ----------

-- Create the structure for the Core Layer Table

CREATE or replace TABLE SalesDWH.core_Sales (
  OrderID INT,
  OrderDate DATE,
  CustomerID INT,
  CustomerName STRING,
  CustomerEmail STRING,
  ProductID INT,
  ProductName STRING,
  ProductCategory STRING,
  RegionID INT,
  RegionName STRING,
  Country STRING,
  Quantity INT,
  UnitPrice DECIMAL(10, 2),
  TotalAmount DECIMAL(10, 2)
);


-- COMMAND ----------

DESCRIBE TABLE SalesDWH.core_Sales;

-- COMMAND ----------

insert into SalesDWH.Core_Sales
select * from salesdwh.trans_Sales

-- COMMAND ----------

select * from SalesDWH.Core_sales

-- COMMAND ----------

-- MAGIC  %md
-- MAGIC  ## SCD
-- MAGIC  

-- COMMAND ----------

create database Sales_SCD;

-- COMMAND ----------

CREATE TABLE Sales_scd.Orders (
  OrderID INT,
  OrderDate DATE,
  CustomerID INT,
  CustomerName STRING,
  CustomerEmail STRING,
  ProductID INT,
  ProductName STRING,
  ProductCategory STRING,
  RegionID INT,
  RegionName STRING,
  Country STRING,
  Quantity INT,
  UnitPrice DECIMAL(10, 2),
  TotalAmount DECIMAL(10, 2)
);

-- COMMAND ----------

--DROP VIEW SALES_SCD.VIEW_DIMPRODUCT
--SELECT * FROM SALES_SCD.view_dimproduct

-- COMMAND ----------

Insert into Sales_scd.Orders(OrderID, OrderDate, customerID, CustomerName, CustomerEmail, ProductID,
ProductName, ProductCategory, RegionID, RegionName, Country, Quantity, UnitPrice, TotalAmount)
VALUES
(1,'2024-02-01',101,'Alice Johnson','alice@example.com',201,'Laptop','Electronics',301,'North America','USA',2,800.00,1600.00),
(2,'2024-02-02',102,'Bob Smith','bob@example.com',202,'Smartphone','Electronics',302,'Europe','Germany',1,500.00,500.00),
(3,'2024-02-03',103,'Charlie Brown','charlie@example.com',203,'Tablet','Electronics',303,' Asia','India',3,300.00,900.00),
(4,'2024-02-04',101,'Alice Johnson','alice@example.com',204,'Headphones','Accessories',301,'North America','USA',1,150.00,150.00),
(5,'2024-02-05',104,'David Lee','david@example.com',205,'Gaming Console','Electronics',302,'Europe','France',1,400.00,400.00),
(6,'2024-02-06',102,'Bob Smith','bob@example.com',206,'Smartphone','Electronics',302,'Asia','China',2,200.00,400.00),
(7,'2024-02-07',105,'Eve Adams','eve@example.com',201,'Laptop','Electronics',301,'North America','Canada',1,800.00,800.00),
(8,'2024-02-08',106,'Frank Miller','frank@example.com',207,'Monitor','Accessories',302,'Europe','Italy',2,250.00,500.00),
(9,'2024-02-09',107,'Grace White','grace@example.com',208,'Keyboard','Accessories',303,'Asia','Japan',3,100.00,300.00),
(10,'2024-02-10',104,'David Lee','david@example.com',209,'Mouse','Accessories',301,'North America','USA',1,50.00,50.00);

-- COMMAND ----------

create or replace table sales_scd.DimProduct
(
  ProductID INT,
  PRODUCTNAME STRING,
  PRODUCTCATEGORY STRING 
)

-- COMMAND ----------

create or replace view sales_scd.view_DimProduct
as 
select distinct(ProductID) ProductID,
ProductName,
ProductCategory
from sales_scd.orders
where OrderDate > '2024-02-10'

-- COMMAND ----------

INSERT INTO sales_scd.DimProduct
SELECT PRODUCTID,PRODUCTNAME,PRODUCTCATEGORY FROM SALES_SCD.view_DimProduct

-- COMMAND ----------

SELECT * FROM sales_SCD.dimproduct

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Upload new records

-- COMMAND ----------

Insert into Sales_scd.Orders(OrderID, OrderDate, customerID, CustomerName, CustomerEmail, ProductID,
ProductName, ProductCategory, RegionID, RegionName, Country, Quantity, UnitPrice, TotalAmount)
VALUES
(1,'2024-02-11',101,'Alice Johnson','alice@example.com',201,'Gaming Laptop','Electronics',301,'North America','USA',2,800.00,1600.00),
(2,'2024-02-12',102,'Bob Smith','bob@example.com',230,'Airpods','Electronics',302,'Europe','Germany',1,500.00,500.00)

-- COMMAND ----------

select * from Sales_scd.view_dimproduct

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # MERGE SCD-01

-- COMMAND ----------

MERGE INTO sales_SCD.dimproduct AS T 
USING SALES_SCD.view_dimproduct S 
ON T.PRODUCTID=S.PRODUCTID
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT  *


-- COMMAND ----------

SELECT * FROM SALES_SCD.dimproduct

-- COMMAND ----------


