/*
===============================================================
Create Database and Schemas
===============================================================
Script Purpose: 
	This script creates a new database named 'DataWarehouse' after checking if it already exists.
	If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
	within the database: 'bronze', 'silver' and 'golden'.
*/

-- USE THE MASTER DB
USE master;
GO

-- DROP AND RECREATE THE 'DataWarehouse' DATABASE
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK INMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO
-- CREATE THE NEW DATABASE
CREATE DATABASE DataWarehouse;
GO
-- USE THE NEW DATABASE CREATED
USE DataWarehouse;
GO
-- CREATE THE SCHEMA FOR THE THREE LAYERS (BRONZE, SILVER AND GOLDEN)
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
