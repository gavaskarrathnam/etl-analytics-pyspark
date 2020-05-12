import mysql.connector
from mysql.connector import errorcode

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType, TimestampType, ShortType, DateType
from pyspark.sql.functions import col


def main():
    # establish a connection to the db
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="autos")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        print("MySQL Connection established", "\n")

    # create a cursor out of a connection; a cursor allows you to communicate with MySQL and execute commands
    cur = conn.cursor()

    spark = initialize_spark()
    df = load_df_with_schema(spark)

    df_cleaned = clean_drop_data(df)
    create_table(cur)

    try:
        insert_query, cars_seq = write_mysql(df_cleaned)
        cur.executemany(insert_query, cars_seq)
        print("Data inserted into MySQL", "\n")

        print("Commiting changes to database", "\n")
        # make sure that your changes are shown in the db
        conn.commit()

        print("'{}' Record(s) inserted successfully into 'autos.cars' table".format(cur.rowcount))

        #fetch data from DB
        get_cars(cur)

    except mysql.connector.Error as error:
        print("Failed to insert record into MySQL table {}".format(error))
    finally:
        if conn.is_connected():
            cur.close()
            print("Closing connection", "\n")
            # close the connection
            conn.close()
            print("Done!", "\n")

#
def initialize_spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Simple etl job") \
        .getOrCreate()

    print("Spark Initialized", "\n")
    return spark


# load data frame with out schema
# def load_DF_WithOut_Schema(spark):
#   df = spark.read.format("csv").option("header", "true").load("/home/gavaskarrathnam/dataeng/etl-analytics-pyspark/data/autos.csv")
#   return df

# Load Data Frame with Schema
def load_df_with_schema(spark):
    schema = StructType([
        StructField("dateCrawled", TimestampType(), True),
        StructField("name", StringType(), True),
        StructField("seller", StringType(), False),
        StructField("offerType", StringType(), True),
        StructField("price", LongType(), True),
        StructField("abtest", StringType(), True),
        StructField("vehicleType", StringType(), True),
        StructField("yearOfRegistration", StringType(), True),
        StructField("gearbox", StringType(), True),
        StructField("powerPS", ShortType(), True),
        StructField("model", StringType(), True),
        StructField("kilometer", LongType(), True),
        StructField("monthOfRegistration", StringType(), True),
        StructField("fuelType", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("notRepairedDamage", StringType(), True),
        StructField("dateCreated", DateType(), True),
        StructField("nrOfPictures", ShortType(), True),
        StructField("postalCode", StringType(), True),
        StructField("lastSeen", TimestampType(), True)
    ])

    df = spark \
        .read \
        .format("csv") \
        .schema(schema)         \
        .option("header", "true") \
        .load("/home/gavaskarrathnam/dataeng/etl-analytics-pyspark/data/autos.csv")

    print("Data loaded into PySpark", "\n")
    return df


def clean_drop_data(df):
    df_dropped = df.drop("dateCrawled","nrOfPictures","lastSeen")
    df_filtered = df_dropped.where(col("seller") != "gewerblich")
    df_dropped_seller = df_filtered.drop("seller")
    df_filtered2 = df_dropped_seller.where(col("offerType") != "Gesuch")
    df_final = df_filtered2.drop("offerType")

    print("Data transformed", "\n")
    return df_final


def create_table(cursor):
    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS autos.cars \
    (   name VARCHAR(255) NOT NULL, \
        price int(11) NOT NULL, \
        abtest VARCHAR(255) NOT NULL, \
        vehicleType VARCHAR(255), \
        yearOfRegistration VARCHAR(4) NOT NULL, \
        gearbox VARCHAR(255), \
        powerPS int(11) NOT NULL, \
        model VARCHAR(255), \
        kilometer int(11), \
        monthOfRegistration VARCHAR(255) NOT NULL, \
        fuelType VARCHAR(255), \
        brand VARCHAR(255) NOT NULL, \
        notRepairedDamage VARCHAR(255), \
        dateCreated DATE NOT NULL, \
        postalCode VARCHAR(255) NOT NULL) PRIMARY KEY (id)) ENGINE=InnoDB AUTO_INCREMENT=1346 DEFAULT CHARSET=utf8;")

        print("Created table in MySQL", "\n")
    except:
        print("Table already exists, good to go!", "\n")


def write_mysql(df):
    cars_seq = [tuple(x) for x in df.collect()]
    records_list_template = ','.join(['%s'] * len(df.columns))
    insert_query = "INSERT INTO autos.cars (name, price, abtest, vehicleType, yearOfRegistration, gearbox, powerPS, \
                        model, kilometer, monthOfRegistration, fuelType, brand, notRepairedDamage, dateCreated, postalCode \
                           ) VALUES ({})".format(records_list_template)
    print("Inserting data into MySQL...", "\n")
    return insert_query, cars_seq


# Get cars data
def get_cars(cursor):

    mySQL_select_Query = "select brand, model, price from autos.cars"
    cursor.execute(mySQL_select_Query)
    cars_records = cursor.fetchmany(2)

    print("Printing 2 rows")
    for row in cars_records:
        print("Brand = ", row[0], )
        print("Model = ", row[1])
        print("Price  = ", row[2], "\n")


if __name__ == '__main__':
    main()