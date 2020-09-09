# AWS Data Migration Tools

This repo holds functions and tools for data migration between AWS services of 
different AWS accounts/regions.

Currently there is only support for AWS DynamoDB. 

The functions can be found in `migrate_dynamodb.py` and the following actions are
supported. 

- Replicate the structure of a DynamoDB. The copy_dynamo_schema function, replicates the current Table structure and 
their schemata. To use you need to pass two session objects as args that correspond to the source and destination 
profiles/regions.

```python
import boto3
from migrate_dynamodb import copy_dynamo_schema as replicator
source = boto3.Session(region_name='eu-central-1', profile_name='test')
dst = boto3.Session(region_name='us-west-2', profile_name='qa')

# Let's replicate everything
replicator(source, dst)

# If I need to replicate just two tables, newTable1 & newTable2
replicator(source, dst, tables=['newTable1','newTable2'])
``` 

- Copy all data from one table in source AWS profile/region to another table in another AWS profile/region. The 
table must exist on the destination and to have the same name with the source.

```python
import boto3
from migrate_dynamodb import migrate_dynamo_data as migrator
source = boto3.Session(region_name='eu-central-1', profile_name='test')
dst = boto3.Session(region_name='us-west-2', profile_name='qa')

# Let's copy everything from scratch
migrator(source, dst)

# Let's copy the data of the previous two tables, newTable1 & newTable2
migrator(source, dst, tables=['newTable1', 'newTable2'])
```

More tools will be added progressively.