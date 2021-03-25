# Code folder for hosting code for a Data Science Project

This folder hosts all code for a data science project. It has three sub-folders, belonging to 3 stages of the Data Science Lifecycle:

1. Data_Exploration
2. Modeling
3. Deployment

The python configuration needs a yaml file for easy access to the ONS database. On top of that it contains frequently used functions regarding at least one of the three stages of the Data Science Lifecycle.

Mentioned yaml file should have the following attributes:

* db_server
* db_port
* db_username
* db_password
* dialect
* driver
* db