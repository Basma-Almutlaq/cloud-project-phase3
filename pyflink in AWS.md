Run the following cell in Jupyter Notebook to confirm you are on the EC2 instance


```python
import socket
print("Running on instance with hostname:", socket.gethostname())

```

    Running on instance with hostname: ip-172-31-19-230.eu-west-2.compute.internal



```python
import os

# Set FLINK_HOME to the path of your Flink directory
os.environ['FLINK_HOME'] = '/home/ec2-user/flink-1.20.0'

# Verify that it's set
print("FLINK_HOME:", os.environ.get('FLINK_HOME'))

```

    FLINK_HOME: /home/ec2-user/flink-1.20.0



```python
import grpc
print("grpc module is installed successfully.")

```

    grpc module is installed successfully.



```python
import apache_beam
print("Apache Beam is installed successfully.")

```

    Apache Beam is installed successfully.



```python
import avro.schema
print("Apache Avro module installed successfully.")

```

    Apache Avro module installed successfully.



```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# Create the Flink environment
env = StreamExecutionEnvironment.get_execution_environment()
env_settings = EnvironmentSettings.new_instance().in_batch_mode().build()
table_env = StreamTableEnvironment.create(env, environment_settings=env_settings)

# Print a confirmation
print("PyFlink environment is successfully set up in Jupyter Notebook!")

```

    <frozen importlib._bootstrap>:228: RuntimeWarning: apache_beam.runners.worker.operations.Operation size changed, may indicate binary incompatibility. Expected 136 from C header, got 144 from PyObject
    <frozen importlib._bootstrap>:228: RuntimeWarning: apache_beam.runners.common.DoFnInvoker size changed, may indicate binary incompatibility. Expected 56 from C header, got 72 from PyObject
    <frozen importlib._bootstrap>:228: RuntimeWarning: apache_beam.runners.common.SimpleInvoker size changed, may indicate binary incompatibility. Expected 72 from C header, got 88 from PyObject
    <frozen importlib._bootstrap>:228: RuntimeWarning: apache_beam.runners.common.PerWindowInvoker size changed, may indicate binary incompatibility. Expected 224 from C header, got 248 from PyObject
    <frozen importlib._bootstrap>:228: RuntimeWarning: apache_beam.runners.common.DoFnRunner size changed, may indicate binary incompatibility. Expected 64 from C header, got 80 from PyObject
    <frozen importlib._bootstrap>:228: RuntimeWarning: apache_beam.runners.worker.operations.ConsumerSet size changed, may indicate binary incompatibility. Expected 64 from C header, got 88 from PyObject
    <frozen importlib._bootstrap>:228: RuntimeWarning: apache_beam.runners.worker.operations.SingletonElementConsumerSet size changed, may indicate binary incompatibility. Expected 72 from C header, got 96 from PyObject
    <frozen importlib._bootstrap>:228: RuntimeWarning: apache_beam.runners.worker.operations.GeneralPurposeConsumerSet size changed, may indicate binary incompatibility. Expected 112 from C header, got 136 from PyObject
    <frozen importlib._bootstrap>:228: RuntimeWarning: apache_beam.runners.worker.operations.ReadOperation size changed, may indicate binary incompatibility. Expected 136 from C header, got 144 from PyObject
    <frozen importlib._bootstrap>:228: RuntimeWarning: apache_beam.runners.worker.operations.ImpulseReadOperation size changed, may indicate binary incompatibility. Expected 144 from C header, got 152 from PyObject
    <frozen importlib._bootstrap>:228: RuntimeWarning: apache_beam.runners.worker.operations.DoOperation size changed, may indicate binary incompatibility. Expected 200 from C header, got 208 from PyObject
    <frozen importlib._bootstrap>:228: RuntimeWarning: apache_beam.runners.worker.operations.SdfProcessSizedElements size changed, may indicate binary incompatibility. Expected 216 from C header, got 224 from PyObject
    <frozen importlib._bootstrap>:228: RuntimeWarning: apache_beam.runners.worker.operations.SdfTruncateSizedRestrictions size changed, may indicate binary incompatibility. Expected 200 from C header, got 208 from PyObject
    <frozen importlib._bootstrap>:228: RuntimeWarning: apache_beam.runners.worker.operations.CombineOperation size changed, may indicate binary incompatibility. Expected 144 from C header, got 152 from PyObject
    <frozen importlib._bootstrap>:228: RuntimeWarning: apache_beam.runners.worker.operations.PGBKCVOperation size changed, may indicate binary incompatibility. Expected 200 from C header, got 208 from PyObject
    <frozen importlib._bootstrap>:228: RuntimeWarning: apache_beam.runners.worker.operations.FlattenOperation size changed, may indicate binary incompatibility. Expected 136 from C header, got 144 from PyObject


    PyFlink environment is successfully set up in Jupyter Notebook!



```python
table_env.execute_sql("DROP TEMPORARY TABLE IF EXISTS ratings_table")
```




    <pyflink.table.table_result.TableResult at 0x7fcd73db87f0>




```python
table_env.create_temporary_table('ratings_table', table_descriptor)
```


    ---------------------------------------------------------------------------

    NameError                                 Traceback (most recent call last)

    Cell In[8], line 1
    ----> 1 table_env.create_temporary_table('ratings_table', table_descriptor)


    NameError: name 'table_descriptor' is not defined



```python
# List all tables to check if the table is registered
result = table_env.execute_sql("SHOW TABLES")
result.print()

```


```python
import pandas as pd

# Load a small sample from S3
df_sample = pd.read_csv('s3://cloud-project-bucket/ratings.csv', nrows=10)

# Display the sample data
print(df_sample)

```


```python
df_sample = df_sample.drop(columns=['timestamp'])
print(df_sample)
```


```python
from pyflink.table import Schema, DataTypes, TableDescriptor

# Drop the temporary table if it exists to avoid the 'already exists' error
table_env.execute_sql("DROP TEMPORARY TABLE IF EXISTS ratings_table")

# Simplified schema definition
schema = Schema.new_builder() \
    .column('userId', DataTypes.INT()) \
    .column('movieId', DataTypes.INT()) \
    .column('rating', DataTypes.FLOAT()) \
    .build()

# Minimal table descriptor for debugging
table_descriptor = TableDescriptor.for_connector('filesystem') \
    .schema(schema) \
    .option('path', 's3://cloud-project-bucket/ratings.csv') \
    .format('csv') \
    .build()

# Register the table
try:
    table_env.create_temporary_table('ratings_table', table_descriptor)
    print("Table registered successfully.")
except Exception as e:
    print(f"Error while registering the table: {e}")

```


```python
import pandas as pd

# Read the CSV file from S3 into a Pandas DataFrame
df = pd.read_csv('s3://cloud-project-bucket/ratings.csv', nrows=10000000)
print(df.head())

```


```python
print(df.isnull().sum())
```


```python
df = df.dropna()
df = df.drop(columns=['timestamp'])
```


```python
print(df.head())
```


```python
print(df.describe())
```


```python
print(df.duplicated().sum())
```


```python
from pyflink.table import EnvironmentSettings, TableEnvironment, Schema, DataTypes, TableDescriptor

# Step 1: Set up the PyFlink environment
env_settings = EnvironmentSettings.new_instance().in_batch_mode().build()
table_env = TableEnvironment.create(env_settings)

# Step 2: Define the schema for the table
schema = Schema.new_builder() \
    .column('userId', DataTypes.INT()) \
    .column('movieId', DataTypes.INT()) \
    .column('rating', DataTypes.FLOAT()) \
    .build()

# Step 3: Create a table descriptor for the DataFrame
table_descriptor = TableDescriptor.for_connector('filesystem') \
    .schema(schema) \
    .option('path', 's3://cloud-project-bucket/ratings.csv') \
    .format('csv') \
    .build()

# Step 4: Register the table
try:
    table_env.create_temporary_table('ratings_table', table_descriptor)
    print("Table registered successfully.")
except Exception as e:
    print(f"Error while registering the table: {e}")

# Step 5: Query the table
result = table_env.execute_sql("SELECT userId FROM ratings_table LIMIT 5")
for row in result.collect():
    print(row)

```




```python

```
