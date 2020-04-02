from impala.dbapi import connect
from impala.util import as_pandas
from impala.error import (Error, InterfaceError, DatabaseError, InternalError,
                         OperationalError, ProgrammingError, IntegrityError,
                         DataError, NotSupportedError)
import pandas as pd
import os
import tempfile
import logging


def generateTemporaryFilename():
    return os.path.join(tempfile.gettempdir(), next(tempfile._get_candidate_names()) + ".csv")


def execute_sql_by_partition(rows_sql, sql, partitions, user, password, host, port):
    try:

        df = ExecuteHiveSQL(rows_sql, user, password, host, port)

        max_rows = df.iloc[0,0] # Max number of rows from the original table.
        factor = (max_rows//partitions)
        print('Number of partitions:', partitions, sep=' ')
        print('Number of rows per partition:', factor, sep=' ')

        # Calculate the number of rows for partitions.
        split_rows = [0]
        for i in range(partitions):
            if i < (partitions - 1):
                num_rows = factor*(i+1)
                split_rows.append(num_rows)
            else:
                num_rows = max_rows+1
                split_rows.append(num_rows)

        # Create a SQL query to limit rows by partitions.
        sql_by_partition = '''
                {0}
                WHERE 
                    en.rowid >= {1} and en.rowid < {2}
                '''

        # Iterate over partitions.
        data = []
        for i in range(partitions):
            print('Execute partition #', i+1, '- from', split_rows[i]
                  ,'to', split_rows[i+1], sep=' ')
            final_sql = sql_by_partition.format(sql, split_rows[i], split_rows[i+1])
            df = ExecuteHiveSQL(final_sql, user, password, host, port)

            # Iterate over the extracted dataframe and append rows to a list.
            for row in df[1:].values.tolist():
                data.append(row)

        # Create a final dataframe from the list
        columns = df.columns
        full_df = pd.DataFrame(data,columns=columns)
        del data, df

        return full_df

    except Exception as e:
        logging.error("execute_sql_by_partition(): Failed - " + str(e),exc_info=True)
        print(e)


def ExecuteHiveSQLLargeFile(sql, outputcsvfilename, user, password, host, port):
    try:
        impala_logger = logging.getLogger('impala')

        #Enable Only CRITICAL Logger
        impala_logger.setLevel(logging.CRITICAL)

        # Estabilish Hive connection
        conn = getHiveConnection(user, password, host, int(port))

        # The returned dataframe will be too large to return in one go - so we need to chunk it up
        # into smaller dataframes and write them as temporary CSV files to disk before rolling back into a single file

        returned = 0
        chunkSize = 50000
        tempFilenames = []

        for chunk in pd.read_sql(sql, con=conn, chunksize=chunkSize):

            returned = returned + chunk.shape[0]
            print("Getting chunk of size: " + str(chunkSize) + " and received " + str(chunk.shape[0]) + " total: " + str(returned) )

            # Generate a temporary filename and add to the array for files to clean up at the end.
            tempFile = generateTemporaryFilename()
            tempFilenames.append(tempFile)

            # Save the chunk to the temporary file
            chunk.to_csv(tempFile,index=False)

        conn.close()
        # Now that we have all the files, roll into a single file.   Use basic python and not pandas due to 
        # the size of the resultant file.
        firstFile = True
        firstLine = True

        # The output file
        try:
            csv_merge = open(outputcsvfilename, 'w', encoding='utf-8')

            # Now loop over the files
            for file in tempFilenames:
                csv_in = open(file,encoding='utf-8')
                for line in csv_in:
                    if firstFile == True:
                        csv_merge.write(line)
                        firstLine = False
                    else:
                        if firstLine == True:
                            firstLine = False
                            continue
                        csv_merge.write(line)
                csv_in.close()
                firstFile = False
                firstLine = True
        finally:
            csv_merge.close()

    except Exception as e:
        logging.error("ExecuteHiveSQLLargeFile(): Failed - " + str(e))
        print(e)

    finally:
        # Do what we can now to remove the temporary files
        for file in tempFilenames:
            print("Deleting temporary file: " + file)
            os.unlink(file)


def ExecuteHiveSQL(sql, user, password, host, port):
    try:
        impala_logger = logging.getLogger('impala')
        
        #Enable Only CRITICAL Logger
        impala_logger.setLevel(logging.CRITICAL)

        ConnectionErrors = (Error, DatabaseError, InternalError, OperationalError,
                            ProgrammingError, IntegrityError, DataError,
                            NotSupportedError)

        # Establish Hive connection
        conn = getHiveConnection(user, password, host, int(port))
        cursor = conn.cursor()
        cursor.execute(sql)
        df_output = as_pandas(cursor)
        conn.close()

        return df_output
    except ConnectionErrors as e:
        logging.error("ExecuteHiveSQL(): Database Error - " + str(e))
        print(e)
        raise
    except Exception as e:
       logging.error("ExecuteHiveSQL(): Failed - " + str(e))
       print(e)


def getHiveConnection(user, password, host, port):
    try:
        # Estabilish Hive connection
        conn = connect(host, port, 
               use_ssl= True, 
               user= user, 
               password=password,
               auth_mechanism='PLAIN')

        return conn
    except Exception as e:
       logging.error("getHiveConnection(): Failed- " + str(e))
       print(e)


if __name__ == "__main__":
    main()
