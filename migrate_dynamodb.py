import threading
from botocore.exceptions import ClientError
from boto3 import client, Session
import backoff
import sys
import copy


def copy_dynamo_schema(src_session: Session, dst_session: Session, tables: list = ()) -> None:
    """
    Reads the DynamoDB in source region and creates the same tables in destination region.

    :param src_session: The aws session object of the source profile/region.
    :param dst_session: The aws session object of the destination profile/region.
    :param tables: A list of the tables to be copied. If empty all tables will be created.
    :return: None
    """

    d_src_client = src_session.client('dynamodb')

    if not tables:
        tables = d_src_client.list_tables()['TableNames']

    for name in tables:
        d_src_resource = src_session.resource('dynamodb')
        table = d_src_resource.Table(name)

        d_dest_resource = dst_session.resource('dynamodb')
        provisioned_throughput = table.provisioned_throughput

        if 'NumberOfDecreasesToday' in provisioned_throughput.keys():
            del provisioned_throughput['NumberOfDecreasesToday']

        params = {
            'TableName': name,
            'KeySchema': table.key_schema,
            'AttributeDefinitions': table.attribute_definitions,
            'ProvisionedThroughput': provisioned_throughput
        }

        if table.global_secondary_indexes:
            gsi = table.global_secondary_indexes
            gsi_copy = copy.deepcopy(gsi)

            for item in gsi:
                for key in item.keys():
                    if key not in ["IndexName", "KeySchema", "Projection", "ProvisionedThroughput"]:
                        del gsi_copy[gsi.index(item)][key]

                if 'NumberOfDecreasesToday' in gsi_copy[gsi.index(item)]['ProvisionedThroughput'].keys():
                    del gsi_copy[gsi.index(item)]['ProvisionedThroughput']['NumberOfDecreasesToday']
            params['GlobalSecondaryIndexes'] = gsi_copy

        d_dest_resource.create_table(**params)
        print(f'Created table {name}')


def migrate_dynamo_data(src_session: Session, dst_session: Session, tables: list = ()) -> None:
    """
    The function copies data from tables in source_region DynamoDB to same tables in destination_region DynamoDB.
    The tables in destination must exist and have the same schema with the source.

    :param src_session: The aws session object of the source profile/region.
    :param dst_session: The aws session object of the destination profile/region.
    :param tables: A list of the tables to be copied. If empty all tables will be copied.
    :return: None
    """

    d_src = src_session.client('dynamodb')

    if not tables:
        tables = d_src.list_tables()['TableNames']

    exclude_list = ['Performance', 'PerformanceMetrics', 'ProductionPipelineErrors']
    tables = [item for item in tables if item not in exclude_list]

    for table in tables:
        t = threading.Thread(target=copy_table_thread, args=(table, src_session, dst_session))
        t.start()


def copy_table_thread(table: str, src_session: Session, dst_session: Session) -> None:

    """
    Spawns a thread that copies data from src_session DynamoDB table to the destination the same table name in
    dst_session.
    :param table: The name of the table to be copied
    :param src_session: The AWS source Session
    :param dst_session: The AWS destination Session
    :return: None
    """

    print(f"Scanning table {table} in source {src_session.region_name}\r")
    d_src = src_session.client('dynamodb')
    d_dst = dst_session.client('dynamodb')

    source_response = d_src.scan(TableName=table)

    try:
        print(f"Scanning table {table} in destination {dst_session.region_name}\r")
        dst_response = d_dst.scan(TableName=table)

    except ClientError:
        print(f"{table} was not found in destination {dst_session.region_name}")
        return

    src_items = source_response['Items']
    dst_items = dst_response['Items']

    count = 1

    if src_items != dst_items:
        items_to_write = [item for item in src_items if item not in dst_items]
        if items_to_write:
            write_items(items_to_write, table, d_dst, count)

    try:
        src_key = source_response['LastEvaluatedKey']
        dst_key = dst_response['LastEvaluatedKey']
    except KeyError:
        src_key = "1"
        dst_key = "2"

    while src_key == dst_key:
        sys.stdout.flush()
        sys.stdout.write(f'\rPage {count} for Table {table} is identical to destination Table')
        source_response = d_src.scan(TableName=table, ExclusiveStartKey=src_key)
        dst_response = d_dst.scan(TableName=table, ExclusiveStartKey=dst_key)

        try:
            src_key = source_response['LastEvaluatedKey']
            dst_key = dst_response['LastEvaluatedKey']
            count += 1
        except KeyError:
            src_items = source_response['Items']
            dst_items = dst_response['Items']
            break

    if src_items != dst_items:
        items_to_write = [item for item in src_items if item not in dst_items]
        if items_to_write:
            write_items(items_to_write, table, d_dst, count)

    while 'LastEvaluatedKey' in source_response:
        key = source_response['LastEvaluatedKey']
        source_response = d_src.scan(TableName=table, ExclusiveStartKey=key)
        items_to_write = source_response['Items']
        write_items(items_to_write, table, d_dst, count)
        # The following counter holds the number of the pages returned from scan function until all items are returned.
        count += 1


def write_items(items_to_write: list, table: str, d_dst: client, page: int) -> None:
    """
    Function that copies items in batches to a specified table.
    :param items_to_write: A list containing the items to be copied
    :param table: The table name that items will be copied into
    :param d_dst: A DynamoDB client of the destination
    :param page: A specified page number to copied.
    :return:
    """

    list_of_items = []
    records_batch = []

    if items_to_write:
        for item in items_to_write:
            record = {'PutRequest': {'Item': item}}
            records_batch.append(record)

            if len(records_batch) == 25:
                list_of_items.append(records_batch)
                records_batch = []

        list_of_items.append(records_batch)

        total_number_of_batches = len(list_of_items)
        count = 1

        for batch in list_of_items:

            if batch:
                request = {table: batch}

                try:
                    r = d_dst.batch_write_item(RequestItems=request)
                    leftovers = r['UnprocessedItems']

                    while leftovers:
                        leftovers = write_records(d_dst, leftovers)
                    sys.stdout.flush()
                    sys.stdout.write(f'\rPage {page} Batch {count}/{total_number_of_batches} for Table {table} copied '
                                     f'successfully!')
                except Exception as e:
                    sys.stdout.flush()
                    sys.stdout.write(f'\rPage {page} Batch {count}/{total_number_of_batches} for Table {table} failed '
                                     f'to copy!: {e}')

            else:
                print('Batch is empty, skipping')
            count += 1
        sys.stdout.write('\n')
    else:
        print('Table {} is empty!'.format(table))


@backoff.on_predicate(backoff.expo(), lambda leftovers: len(leftovers) > 0, jitter=backoff.full_jitter)
def write_records(dst, records):
    r = dst.batch_write_item(RequestItems=records)
    return r['UnprocessedItems']


def get_total_items(table: str, aws_session: Session) -> list:
    """
    A helper function to get programmatically all items from a table and
    :param table: The table name to retrieve its items
    :param aws_session: The AWS Session of the targeted AWS profile/region
    :return: A list that contains all items
    """
    d_client = aws_session.client('dynamodb')
    print(f"Scanning table {table} in {aws_session.region_name}")

    try:
        response = d_client.scan(TableName=table)
    except ClientError:
        print(f"{table} was not found in {aws_session.region_name}")
        return []
    items = response['Items']
    total_items = items

    while 'LastEvaluatedKey' in response:
        key = response['LastEvaluatedKey']
        src_response = d_client.scan(TableName=table, ExclusiveStartKey=key)
        total_items.extend(src_response['Items'])

    print(f"Scanned table {table} in {aws_session.region_name}!")

    return total_items
