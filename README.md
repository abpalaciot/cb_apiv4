Configuring and Setting
=========
## Setting Up the Database and API key
Before executing the function set up the API Key and the Database using any of the following methods:

1. Using the environment variables e.g.,:
   
    * `$ export MONGO_URL='mongodb://127.0.0.1:27017'`
    * `$ export CB_API_KEY='XXXXXX'`
    * `$ export MONGO_DB_NAME='CB_SCR_DB'`
2. Modify the `MONGO_URL`, `API_KEY`, `DATABASE_NAME`, variables in the `utils.py` file

## On local Machine

To run the functions on your local machine:

1. Install the packages in the `local_requirements.txt`
2. Execute `$ functions_framework --debug --target=<function name>`. 


        
