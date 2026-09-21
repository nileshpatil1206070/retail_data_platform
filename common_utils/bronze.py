
import pyspark.sql.functions as F
from common_utils.logging import get_logger

logger = get_logger('common_utils_bronze')

def read_raw(spark,raw_path,file_format, options = None):

    """
    read raw files from volume
    raw_path : 
    file_format: 'json' or 'csv' or 'parquet'
    options: key value

    returns data frame
    """
    logger.info('reading from path: %s', raw_path)
    logger.info('file_format: %s' , file_format)

    reader = spark.read.format(file_format)
    for key,value in (options or {}).items():
        reader = reader.option(key,value)

    return reader.load(raw_path)
    logger.info('read complete and data stored in dataframe df')



def bronze_ingestor(df, mode , file_format, target_table):

    """
    add audit columns n write in bronze as delta table
    df: dataFrame read from Volume
    mode: overwrite/append depends on situation
    target_table:catalog.schema.table
    return the number of rows
    """
    logger.info('adding columns in dataframe')
    

    df = df.withColumn("last_update_ts", F.current_timestamp() )\
        .withColumn("file_path", F.col("_metadata.file_path"))

    logger.info('writing delta table: %s' , file_format)
    logger.info('selected mode is : %s' , mode)
    df.write.format('delta').mode(mode).saveAsTable(target_table)
    
    return df.count()






