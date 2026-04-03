# AWS Glue Connection Setup (SQL Server – Global Partners)

## In case you need to uplaod drivers 

-  Download from Microsoft website 
https://learn.microsoft.com/en-us/sql/connect/jdbc/microsoft-jdbc-driver-for-sql-server?view=sql-server-ver17

- Extract and upload  mssql-jdbc-12.4.2.jre11.jar fiel to S3 globalpartner-glue-scripts

# Create Glue Connection

**Before setting up this make sure your VPC configrations are correct**
**Glue Security Group, Subnet, Endpoints should be linked properly same as RDS**



