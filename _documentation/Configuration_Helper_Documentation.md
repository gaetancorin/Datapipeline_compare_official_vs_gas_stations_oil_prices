# Configuration Helper Documentation

This documentation help you to manage your global configuration.

### - [Increase WSL Memory (GB) to Load All Data](#increase-wsl-memory-gb-to-load-all-data)
### - [MongoDB User Rights Management and Creation](#mongodb-user-rights-management-and-creation)
   - [Create an Admin Account](#create-an-admin-account-on-mongo)
   - [Enable MongoDB Authentication (User/Password)](#enable-mongodb-authentication-userpassword)
   - [Disable MongoDB Authentication  (User/Password)](#disable-mongodb-authentication-userpassword)
   - [Create a New MongoDB User](#create-a-new-mongodb-user)
   - [Delete a MongoDB User](#delete-a-mongodb-user)
   - [Example Connection URIs](#example-connection-uris)
### - [Connect MongoDB to Metabase (Locally and in the Cloud)](#connect-mongodb-to-metabase-locally-and-in-the-cloud)
   - [Connect MongoDB Atlas (cloud) to Metabase](#connect-mongodb-atlas-cloud-to-metabase)
   - [Connect MongoDB (locally) to Metabase](#connect-mongodb-locally-to-metabase)

----

## Increase WSL Memory (GB) to Load All Data
Create a file at the following location:
```
C:\Users\{username}\.wslconfig
```
Add the following content to the file:
```
[wsl2]
memory=15GB
processors=6
swap=0
localhostForwarding=true
```
It is recommended to allocate at least 15GB of memory and 6 processors so that all data can be loaded.

Restart WSL using PowerShell:
```
wsl --shutdown
```

---

## MongoDB User Rights Management and Creation
- Ouvrir mongoDB Compass: [mongodb://localhost:27017/](mongodb://localhost:27017/)

### Create an Admin Account on Mongo:
- Open the MongoDB Shell in MongoDB Compass:
    ```
    use admin
    ```

    ```
    db.createUser({
    user: "admin",
    pwd: "password",
    roles: [ { role: "root", db: "admin" } ]
    })
    ```
  
    ```
    db.getUsers()
    ```

### Enable MongoDB Authentication (User/Password)
(⚠️ It is mandatory to have at least one Admin Account to enforce MongoDB user/password authentication. If you do not have an admin account, you will permanently lose access to the database ⚠️)
- On Windows, open the file `C:\Program Files\MongoDB\Server\8.0\bin\mongod.cfg`
- Add the following on the file:
    ```
    security:
        authorization: "enabled"
    ```
- Open PowerShell as Administrator and restart MongoDB from the command line:
    ```
    net stop MongoDB
    ```
    ```
    net start MongoDB
    ```

### Disable MongoDB Authentication  (User/Password)
- On Windows, open the file `C:\Program Files\MongoDB\Server\8.0\bin\mongod.cfg`
- Add the following on the file:
    ```
    security:
        authorization: "disabled"
    ```
- Open PowerShell as Administrator and restart MongoDB from the command line:
    ```
    net stop MongoDB
    ```
    ```
    net start MongoDB
    ```

### Create a New MongoDB User:
- Open MongoDB Compass: [mongodb://localhost:27017/](mongodb://localhost:27017/)

- Open the MongoDB Shell in MongoDB Compass:
    ```
    use admin
    ```
- Create a Read/Write user:
  ```
    db.createUser({
      user: "read_write",
      pwd: "password",
      roles: [
        { role: "readWrite", db: "datalake" },
        { role: "readWrite", db: "denormalization" }
      ]
    })
    ```
- Create a Read-Only user:
  ```
    db.createUser({
      user: "read_only",
      pwd: "password",
      roles: [
        { role: "read", db: "datalake" },
        { role: "read", db: "denormalization" }
      ]
    })
    ```
    ```
  db.getUsers()
    ```
### Delete a MongoDB User:
- Open MongoDB Compass: [mongodb://localhost:27017/](mongodb://localhost:27017/)
- Open the MongoDB Shell in MongoDB Compass:
    ```
    use admin
    ```
    ```
    db.getUsers()
    ```
    ```
    db.dropUser("read_write")
    db.dropUser("read_only")
    ```

### Example Connection URIs:
- "mongodb://localhost:27017/"
- "mongodb://admin:password@localhost:27017/"
- "mongodb://read_write:password@localhost:27017"
- "mongodb://read_only:password@localhost:27017"

----

## Connect MongoDB to Metabase (Locally and in the Cloud)

### Connect MongoDB Atlas (cloud) to Metabase
Follow the official Metabase tutorial to learn how to connect MongoDB Atlas:
https://www.youtube.com/watch?v=3TU9IXRlH24

### Connect MongoDB (locally) to Metabase
- In Metabase, navigate to
**Settings → Admin Settings → Databases → Add a Database**

- Fill in the connection details and save

(This is an example of the fields to fill in to connect MongoDB locally with Metabase running in a Docker container)
<img src="./assets/configuration_helper_documentation/connect_mongo_to_metabase_locally.png" alt="Connect Metabase To Mongo Locally" width="450"/>
