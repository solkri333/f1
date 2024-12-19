import mysql.connector
from mysql.connector import Error
import warnings
import logging
logging.basicConfig(level=logging.INFO)
warnings.filterwarnings("ignore")


def select_year(connection):
    cursor=None
    try:
        select_query=f"""
            select distinct Year(date) from seasons;
        """
        cursor=connection.cursor()
        cursor.execute(select_query)
        result=cursor.fetchall()
        # print(result)
        result.sort(reverse=True)
        return result
    except Error as e:
        logging.error(f"Error:{e}")
    except Exception as e:
        logging.error(f"Error:{e}")
    finally:
        if cursor:
            cursor.close()

def select_grandprix(year, connection):
    cursor=None
    try:
        select_query=f"""
            select grandprix,seasonid from seasons where year(date)={year} order by date DESC;
        """
        cursor=connection.cursor()
        cursor.execute(select_query)
        result=cursor.fetchall()
        # print(result)
        return result
    except Error as e:
        logging.error(f"Error:{e}")
    except Exception as e:
        logging.error(f"Error:{e}")
    finally:
        if cursor:
            cursor.close()

def select_rank(seasonid, connection):
    cursor=None
    try:
        #insert query
        select_query = f"""
            select `rank`, p.name, c.img, laps, time, points, picture, t.img, t.name from 
            players p inner join rankings r on p.playerid=r.playerid
            inner join country c on p.country=c.name
            inner join team t on p.team=t.name
            where r.seasonid={seasonid} order by `rank` 
        """
        cursor = connection.cursor()
        cursor.execute(select_query)
        result=cursor.fetchall()
        # i=0
        # for res in result:
        #     i+=1
        #     print(res)
        # print(str(i)+" rows")
        return result
    except Error as e:
        logging.error(f"Error: {e}")
        return f"Error: {e}", 500
    
    except Exception as e:
        logging.error(f"Error: {e}")
        return f"Error: {e}", 500


def player(connection, playerid):
    cursor=None
    try:
        select_query=f"""
            select name, country, team from players where playerid={playerid};
        """
        select_query2=f"""
            select `rank`, grandprix, Year(date) from rankings r inner join seasons s on r.seasonid=s.seasonid where playerid={playerid} order by date
        """
        cursor=connection.cursor()
        cursor.execute(select_query)
        result=cursor.fetchall()
        cursor.execute(select_query2)
        result2=cursor.fetchall()
        print(result)
        print(result2)
        return result, result2
    except Error as e:
        logging.error(f"Error:{e}")
    except Exception as e:
        logging.error(f"Error:{e}")

def connect():
    try:
        # Establish a database connection
        connection = mysql.connector.connect(
            host='localhost',
            database='f1',
            user='root',
            password='12345@123455'
        )

        if connection.is_connected():
            # print("Connected to the database")
            # select_rank(4, connection)
            # select_year(connection)
            # select_grandprix(2024, connection)
            return connection
        else:
            raise Exception("Failed to connect to the database")

    except Error as e:
        logging.error(f"Error while connecting to MySQL: {e}")
        return f"Error while connecting to MySQL: {e}", 500

    except Exception as e:
        logging.error(f"Error while connecting to MySQL: {e}")
        return f"Error while connecting to MySQL: {e}", 500
