# Amazon RDS + SSMS

## Create the RDS Instance

 - Search Aurora and RDS
 - Open Databases
 - Click Create database
 - Select FUll configrations
 - Select Micosoft SQL Server
 - Templates: Free tier (if Express) or Dev/Test
 - Database management type -> Amazon RDS
 - Edition -> SQL Server Express Edition
 - Version: SQL Server 2019 or 2022 — latest available
 - DB instance identifier -> globalpartners-db
 - Credentials management -> Self
 - Master username - > admin (or your choice) -> Write this down — needed for SSMS
 - Master password -> Strong password 12+ chars -> Write this down — needed for SSMS
 - DB instance class -> Burstable classes (includes t classes)
 - DB instance type->	db.t3.medium	
 - Storage type ->  gp2 (General Purpose SSD) or gp3 (General Purpose SSD)
 - Allocated storage -> 20GB
 - Provisioned IOPS 3000
 - Storage throughput 125
 - Compute resource -> Dont connect to an EC2 compute resouce
 - **Virtual private cloud (VPC)** Click Default or create new and setup ( Important step as SG and subnet should be same here and on GLue to work)
 - **DB subnet group**  Click Default or create new and setup ( Important step as SG and subnet should be same here and on GLue to work)
 - Public accessibility -> Yes -> So SSMS can connect
 - VPC security group (firewall) -> Choose existing
 - Existing VPC security groups -> default
 - Certificate authority - optional -> default
 - Add tags optional
 - Monitoring -> Standard/default
 - Performance Insights -> uncheck
 - Additional monitoring settings -> optiona to turn on Enhanced Monitoring
 - All other settings remain default
 - Click Create and it will take 10-20 mins

## Security Group — Allow your laptop to connect

- Open port 1433 so SSMS on your laptop can reach RDS in case it's blocked
- RDS Console → click your database → scroll to Security → click the VPC security group link
- Click "Inbound rules" tab → "Edit inbound rules" → "Add rule"
- Rule 1: Type = MS SQL · Port = 1433 · Source = My IP (auto-fills your laptop IP)
- Rule 2: Type = MS SQL · Port = 1433 · Source = Custom · enter your Glue VPC CIDR (e.g. 10.0.0.0/16) — so Glue jobs can connect later
- You can also try these if any issues
- All Trafic, all, all, default SG
- MSQL, TCP, 1433, default SG
- All Trafic, all, all, 4.37.54.43/32
- All TCp, TCP, 0-65535, default SG
- Click Save rules

  
###  Security Group – Inbound Rules

<p align="center">
  <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/Security%20group%20inbound%20rules.png" width="750"/>
</p>



## VPC

We will Create 3 Endpoints 

- **STS endpoint**
- **Secrets Manager endpoint**
- **S3 gateway endpoint**

- Open VPC
- Click PrivateLink and Lattice -> Endpoint
- **Create Endpoint**
- Name tag - S3 gateway endpoint
- Type ->  AWS service
- Service -> com.amazonaws.us-east-1.s3 Gateway
- Network settings -> VPC -> Select same the one you used in RDS default or the one you created
- Route tables ->  Select the available route table so it will have subnets available to use and make sure to Explicit subnet associations the one you using in glue connection
- Policy -> Full access
- Create endpoint

  
- **Create Endpoint**
- Name tag - STS endpoint
- Type ->  AWS service
- Service -> com.amazonaws.us-east-1.sts Interface
- Network settings -> VPC -> Select same the one you used in RDS default or the one you created
- Private DNS name -> Enabled
- DNS record IP type -> IPv4
- Subnets -> Select the subnet you selected for route table or select multiple
- Security groups -> Select same the one you used in RDS default or the one you created
- Policy -> Full access
- Create endpoint

- **Create Endpoint**
- Name tag - Secrets Manager endpoint
- Type ->  AWS service
- Service -> com.amazonaws.us-east-1.secretsmanager Interface
- Network settings -> VPC -> Select same the one you used in RDS default or the one you created
- Private DNS name -> Enabled
- DNS record IP type -> IPv4
- Subnets -> Select the subnet you selected for route table or select multiple
- Security groups -> Select same the one you used in RDS default or the one you created
- Policy -> Full access
- Create endpoint

###  Route Table Setup
<p align="center">
  <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/Route%20table.png" width="750"/>
</p>

### Subnets Configuration
<p align="center">
  <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/subnets%20.png" width="750"/>
</p>

###  VPC 

<p align="center">
  <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/VPC.png" width="750"/>
</p>

## Get Your Connection Details

- RDS Console → Databases → click globalpartners-db
- Scroll to "Connectivity & security" section
- Copy these two values — save them somewhere:
   - Endpoint: globalpartners-db.xxxxxxxxx.us-east-1.rds.amazonaws.com
   - Port: 1433
- Full server name for SSMS: You paste the endpoint as the server name in SSMS. It will look like:
  - globalpartners-db.xxxxxxxxx.us-east-1.rds.amazonaws.com,1433 — include the comma and port number.

# SSMS Connection to RDS

## Install SSMS (if not already done)

- Download and install SQL Server Management Studio 19+
- learn.microsoft.com/en-us/sql/ssms/download-sql-server-management-studio-ssms
- Restart your computer after install completes
- Object Explorer → Connect → Database Engine
- Open SSMS → "Connect to Server" dialog appears automatically
- Fill in the connection dialog:
    - Server type: Database Engine
    - Server name: globalpartners-db.xxxxxxxxx.us-east-1.rds.amazonaws.com,1433
    - Authentication: SQL Server Authentication
    - Login: admin (or your master username)
    - Password: your master password
- Set "Encrypt connection" = Mandatory · Set "Trust server certificate" = YES — without this you will get an SSL error
- Click Connect










