# Pintrest Data Pipeline

### Scenario:

Pinterest crunches billions of data points every day to decide how to provide more value to their users. This is an attempt to create a (simplified) similar system using AWS Cloud.

### 

 This code base is concerned with 
 creating inputs and outputs for Pintrest-style data. And interacting with 
 creating sample data, similar to Pintrest outputs. 

There's two main 

Batch Processing
Streaming Data.

## Scenario Architecture

![The diagram of the architecture used in the project.](img/Pintrest_pipeline_arch.jpg)


## Local Installation

To grab the code:

```
git clone https://github.com/spence91-aicore/pinterest-data-pipeline595.git
```

### Local Dependancies

For the files that can be run locally, the project dependencies can be found in the `environment.yml` file.

If you have Conda installed, you can install an environment using the following command.

```
conda env create -f .\environment.yml 
```

Please note - other package versions **may** work fine, but the versions outlined is what **has been tested**.

If you want to run the notebooks then you'll also need to have ipykernel installed as well.

```
python -m pip install ipykernel==6.29
```

### Local Config

The two files concerned with emulating user posts: `user_posting_emulation`, and `user_posting_emulation_streaming` require a database connection to pull sample data. This detail can be put in a file in the root directory of the project, called `db_creds.yml`

e.g

```
cd pintrest_data_pipeline
touch db_creds.yml
```

Example `db_creds.yml` file, replace all `$....` with actual details.

```
RDS_HOST: $PINTERESTDB
RDS_PASSWORD: $PINTERESTDBPW
RDS_USER: $DBUSER
RDS_DATABASE: $DBNAME
RDS_PORT: $DBPORT
```

