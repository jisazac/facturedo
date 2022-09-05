import os
import boto3
import awswrangler as wr

def run_athena_query(query, database="facturedo", s3_output="s3://facturedobucket/data/", boto3_session=None, categories=None, chunksize=None, ctas_approach=None, profile=None, workgroup='primary', region_name='us-east-1', keep_files=False, max_cache_seconds=0):
    """
    An end 2 end Athena query method, based on the AWS Wrangler package. 
    The method will execute a query and will return a pandas dataframe as an output.
    you can read more in https://aws-data-wrangler.readthedocs.io/en/stable/stubs/awswrangler.athena.read_sql_query.html

    Args:
        - query: SQL query.

        - database (str): AWS Glue/Athena database name - It is only the original database from where the query will be launched. You can still using and mixing several databases writing the full table name within the sql (e.g. database.table).

        - ctas_approach (bool): Wraps the query using a CTAS, and read the resulted parquet data on S3. If false, read the regular CSV on S3.

        - categories (List[str], optional): List of columns names that should be returned as pandas.Categorical. Recommended for memory restricted environments.

        - chunksize (Union[int, bool], optional): If passed will split the data in a Iterable of DataFrames (Memory friendly). If True wrangler will iterate on the data by files in the most efficient way without guarantee of chunksize. If an INTEGER is passed Wrangler will iterate on the data by number of rows igual the received INTEGER.

        - s3_output (str, optional): Amazon S3 path.

        - workgroup (str, optional): Athena workgroup. 

        - keep_files (bool): Should Wrangler delete or keep the staging files produced by Athena? default is False

        - profile (str, optional): aws account profile. if boto3_session profile will be ignored.

        - boto3_session (boto3.Session(), optional): Boto3 Session. The default boto3 session will be used if boto3_session receive None. if profilename is provided a session will automatically be created.

        - max_cache_seconds (int): Wrangler can look up in Athena history if this query has been run before. If so, and its completion time is less than max_cache_seconds before now, wrangler skips query execution and just returns the same results as last time. If reading cached data fails for any reason, execution falls back to the usual query run path. by default is = 0

    Returns:
        - Pandas DataFrame

    """
    # test for boto3 session and profile.
    if ((boto3_session == None) & (profile != None)):
        boto3_session = boto3.Session(profile_name=profile, region_name=region_name)

    print("Querying AWS Athena...")

    try:
        # Retrieving the data from Amazon Athena
        athena_results_df = wr.athena.read_sql_query(
            query,
            database=database,
            boto3_session=boto3_session,
            categories=categories,
            chunksize=chunksize,
            ctas_approach=ctas_approach,
            s3_output=s3_output,
            workgroup=workgroup,
            keep_files=keep_files,
            max_cache_seconds=max_cache_seconds
        )

        print("Query completed, data retrieved successfully!")
    except Exception as e:
        print(f"Something went wrong... the error is:{e}")
        raise Exception(e)

    return athena_results_df

