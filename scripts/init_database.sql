/*
=========================================
Create Database and Schemas
=========================================
This script prepares the SQL Server environment for a new data warehouse setup. It first switches to the master database so that administrative operations can be performed. 
The script then checks whether a database named DataWarehouse already exists. If it does, the database is forced into single-user mode, and any active connections are terminated 
to ensure safe modification or recreation.
After ensuring the environment is ready, the script creates a fresh DataWarehouse database and switches the context into it.Inside this 
new database, three schemas—bronze, silver, and gold—are created. These schemas represent the layered structure of a modern data 
engineering architecture, where bronze holds raw data, 
silver contains cleaned and transformed data, and gold stores analytics-ready data models.
This setup forms the foundational structure for building a scalable and well-organized data warehouse.

Warning:
Running this script will forcefully drop and recreate the DataWarehouse database.
All existing data, tables, schemas, and objects inside the database will be permanently deleted.
Use this script only in development or test environments, or ensure you have taken proper backups before executing it.
*/


USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create schema
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
