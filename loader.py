import csv
import pdb
import psycopg2
import re
import argparse

TABLE_NAME = "transactions"

def connect_to_db(database, user):
    conn = psycopg2.connect(dbname=database, user=user, host="localhost")
    conn.autocommit = True
    return conn.cursor()

def parse_args():
    parser = argparse.ArgumentParser(description="load data into database")
    parser.add_argument("-f", "--file", type=str, help="file to upload")
    parser.add_argument("-d", "--database", type=str, help="name of database in postgres")
    parser.add_argument("-u", "--user", type=str, help="name of database user")
    return parser.parse_args()

def main(args):
    with open(args.file, 'rU') as csvfile:
        pg = connect_to_db(args.database, args.user)

        print "Truncating transactions table"
        truncate_sql = """
        TRUNCATE TABLE transactions;
        """
        pg.execute(truncate_sql)
        
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        header = False
        for row in reader:
            if not header:
                header = True
                pass
            else:
                date_of_sale = row[0]
                brand = row[2]
                category = row[3]
                season = row[4]
                sku = row[5]
                description = row[6]
                cost = row[7]
                price = row[8]

                if date_of_sale != "":
                    sql = """
                    INSERT INTO {table} (date_of_sale, brand, category, season, sku, description, cost, price)
                    VALUES ('{date}', '{brand}', '{category}', '{season}', '{sku}', '{description}', {cost}, {price})
                    """.format(
                            table=TABLE_NAME, 
                            date=date_of_sale, 
                            brand=brand, 
                            category=category, 
                            season=season, 
                            sku=sku,
                            description=description, 
                            cost=cost, 
                            price=price)
                    try:
                        pg.execute(sql)
                    except Exception, e:
                        print "Error on row {}".format(row)
                        print e


if __name__ == "__main__":
    args = parse_args()
    main(args)
