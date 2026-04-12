# AWS Glue Connection Setup (SQL Server – Global Partners)

## In case you need to uplaod drivers 

-  Download from Microsoft website 
https://learn.microsoft.com/en-us/sql/connect/jdbc/microsoft-jdbc-driver-for-sql-server?view=sql-server-ver17

- Extract and upload  mssql-jdbc-12.4.2.jre11.jar fiel to S3 globalpartner-glue-scripts

# Create Glue Connection

- **Before setting up this make sure your VPC configrations are correct**
- **Glue Security Group, Subnet, Endpoints should be linked properly same as RDS**
- **Make sure your role has permissions EC2 Full and GlueServiceRole**
  

# Glue connection Setup


- Open AWS Glue -> Click connections -> Create Connection
- Choose data source -> Microsoft SQL Server -> next
- Database instances -> Your RDS database select that
- Database name -> The one you created in SSMS
- Credential type -> AWS Sewcret Manager and select your secret name
- IAM service role -> The one which has all permissions as mentioned above
- Network options
    -  VPC -> Same as RDS
    -  Subnet -> in Subnets Configuration we had 6 subnets available, select the one which you select in the route as Explicit subnet associations
    -  Security groups -> same as RDS
- Click next
- Name -> Sqlserver connection
- Click next and revew and create connection
- Once created, select and click action and test connection

##  AWS Glue Connection – RDS Integration

<p align="center">
  <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/glue%20connection.png" width="750"/>
</p>

  

 

