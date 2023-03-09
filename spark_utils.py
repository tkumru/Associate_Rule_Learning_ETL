import logging
import sys

def setup_logger(name=__name__):
    logger = logging.getLogger(__name__)
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)

    c_handler = logging.StreamHandler(sys.stdout)
    
    c_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    c_handler.setFormatter(c_format)

    logger.addHandler(c_handler)

    return logger

def write_psql(df, db_name: str="test", 
                      mode: str="append", 
                      table_name: str="test", 
                      user: str="postgres", 
                      password: str="password"):
    """
    Function gets dataframe from spark. It saves dataframe to 
    p-sql selected database.table. It can be used with Spark Streaming.

    Parameters
    ----------
    df : Any dataframe
        Mostly used with spark dataframe.
    db_name : str, optional
        P-SQL database name. The default is "test".
    mode : str, optional
        It can be append, overwrite and ext. The default is "append".
    table_name : str, optional
        P-SQL table name. The default is "test".
    user : str, optional
        P-SQL user name. The default is "postgres".
    password : str, optional
        P-SQL password. The default is "password".
    """
    logg = setup_logger()
    logg.info("write_psql function is started.")
    
    jdbc_url = f"jdbc:postgresql://localhost:5432/{db_name}"
    
    try:
        (
             df.write
                 .format("jdbc")
                 .mode(mode)
                 .options(
                         url=jdbc_url,
                         dbtable=table_name,
                         user=user,
                         password=password,
                         driver="org.postgresql.Driver"
                     )
                 .save()
         )
        logg.info("PostgreSQL {db_name}.{table_name} is saved.")
    except Exception as e:
        logg.error(f"Dataframe cannot {mode} to {db_name}.{table_name}!\n{e}")
    finally:
        logg.info("write_psql function was finished.")

def write_hive(df, format_: str="orc", 
               mode: str="overwrite", 
               db_name: str="test", 
               table_name: str="test"):
    """
    Function gets dataframe from spark. It saves dataframe to 
    apache hive selected database.table. It can be used with 
    Spark Streaming.

    Parameters
    ----------
    df : Any dataframe
        Mostly used with spark dataframe.
    format_ : str, optional
        Select to which saved file type. Best choice is orc
        for apache hive. The default is "orc".
    mode : str, optional
        It can be append, overwrite and ext. The default is "overwrite".
    db_name : str, optional
        Apache hive database name. The default is "test".
    table_name : str, optional
        Apache hive table name. The default is "test".
    """
    logg = setup_logger()
    logg.info("write_hive function is started.")
    
    try:
        (
             df.write
                 .format(format_)
                 .mode(mode)
                 .saveAsTable(f"{db_name}.{table_name}")
         )
    except Exception as e:
        logg.error(f"Dataframe cannot {mode} to {db_name}.{table_name}!\n{e}")
    finally:
        logg.info("write_hive function was finished.")
        
def write_deltalake(df, mode: str="overwrite", 
                    path: str="/user/talha/delta_db/"):
    """
    Function gets dataframe from spark. It saves dataframe to 
    deltalake selected path. It can be used with Spark Streaming.

    Parameters
    ----------
    df : Any dataframe
        Mostly used with spark dataframe.
    mode : str, optional
        It can be append, overwrite and ext. The default is "overwrite".
    path : str, optional
        Deltalake path save is where. The default is "/user/talha/delta_db/".
    """
    logg = setup_logger()
    logg.info("write_deltalake function is started.")
    
    try:
        (
            df.write
                .format("delta")
                .mode("overwrite")
                .save(path)
        )
    except Exception as e:
        logg.error(f"Dataframe cannot {mode} to {path}!\n{e}")
    finally:
        logg.info("write_deltalake function was finished.")
        
def upsert_deltalake(df, new_df, join_expr: str):
    """
    Function gets dataframes from spark. Function upserts dataframe to
    to other dataframe. It can be used with Spark Streaming.

    Parameters
    ----------
    df : Any dataframe
        Mostly used with spark dataframe.
    new_df : Any dataframe
        Mostly used with spark dataframe.
    join_expr : str
        Matched join expression. 
        Example: df.customerId == new_df.customerId
    """
    logg = setup_logger()
    logg.info("upsert_deltalake function is started.")
    
    try:
        logg.warn("Old dataframe alias: df\nNew dataframe alias:new_df")
        (
             df.alias("df") \
                 .merge(new_df.alias("new_df"), join_expr) \
                 .whenMatchedUpdateAll() \
                 .whenNotMatchedInsertAll() \
                 .execute()
         )
    except Exception as e:
        logg.error(f"Dataframe cannot upserted to dataframe!\n{e}")
    finally:
        logg.info("upsert_deltalake function was finished.")
        
def read_psql(spark, db_name: str="test", 
              table_name: str="test", 
              user: str="postgres", 
              password: str="password"):
    """
    Function reads table from p-sql.

    Parameters
    ----------
    spark : SparkSession
        SparkSession application.
    db_name : str, optional
        P-SQL database name. The default is "test".
    table_name : str, optional
        P-SQL table name. The default is "test".
    user : str, optional
        P-SQL user name. The default is "postgres".
    password : str, optional
        P-SQL password. The default is "password".

    Returns
    -------
    Spark.Dataframe
    """
    logg = setup_logger()
    logg.info("read_psql function is started.")
    
    jdbc_url = f"jdbc:postgresql://localhost:5432/{db_name}"
    
    try:
        return (
             spark.read 
                .format("jdbc") 
                .options(
                    url=jdbc_url,
                    dbtable=table_name,
                    user=user,
                    password=password,
                    driver="org.postgresql.Driver") 
                .load()
        )
    except Exception as e:
        logg.error(f"Dataframe cannot read from p-sql!\n{e}")
    finally:
        logg.info("read_psql function was finished.")
        
def read_hive(spark, db_name: str="test", 
              table_name: str="test"):
    """
    Function reads table from apache hive.

    Parameters
    ----------
    spark : SparkSession
        SparkSession application.
    db_name : str, optional
        Apache hive database name. The default is "test".
    table_name : str, optional
        Apache hive table name. The default is "test".

    Returns
    -------
    Spark.Dataframe
    """
    logg = setup_logger()
    logg.info("read_hive function is started.")
    
    try:
        return spark.sql(f"select * from {db_name}.{table_name}")
    except Exception as e:
        logg.error(f"Dataframe cannot read from apache hive!\n{e}")
    finally:
        logg.info("read_hive function was finished.")
        
def read_delta(spark, path: str):
    """
    Function reads table from deltalake.

    Parameters
    ----------
    spark : SparkSession
        SparkSession application.
    path : str
        Deltalake path.

    Returns
    -------
    Spark.Dataframe
    """
    logg = setup_logger()
    logg.info("read_delta function is started.")
    
    try:
        return (
                    spark.read
                        .format("delta")
                        .load(path)
                )
    except Exception as e:
        logg.error(f"Dataframe cannot read from deltalake!\n{e}")
    finally:
        logg.info("read_delta function was finished.")
