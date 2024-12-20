import psycopg2
from psycopg2 import Error
import warnings
import logging
logging.basicConfig(level=logging.INFO)
warnings.filterwarnings("ignore")


def select_year(connection):
    cursor=None
    try:
        select_query=f"""
            SELECT DISTINCT EXTRACT(YEAR FROM date) AS year FROM seasons;
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
             SELECT grandprix, seasonid FROM seasons WHERE EXTRACT(YEAR FROM date) = {year} ORDER BY date DESC;
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
            SELECT "rank", p.name, c.img, laps, time, points, picture, t.img, t.name 
            FROM players p 
            INNER JOIN rankings r ON p.playerid = r.playerid
            INNER JOIN country c ON p.country = c.name
            INNER JOIN team t ON p.team = t.name
            WHERE r.seasonid = {seasonid}
            ORDER BY "rank"; 
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
            SELECT name, country, team FROM players WHERE playerid={playerid};
        """
        select_query2=f"""
            SELECT "rank", grandprix, EXTRACT(YEAR FROM date) AS year 
            FROM rankings r 
            INNER JOIN seasons s ON r.seasonid = s.seasonid 
            WHERE playerid={playerid} 
            ORDER BY date;
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
        # PostgreSQL connection string
        connection = psycopg2.connect(
            host="dpg-c8o0s6dskk8l0f4c0000-a.oregon-postgres.render.com",  # Remote host (Render's database URL)
            port="5432",  # Default port
            database="f1_n2bx",  # Your database name
            user="root",  # Your PostgreSQL user
            password="3K661l1VMQV5v5b2zQMor4KuH3xfYQ4S",  # Your PostgreSQL password
            sslmode="require"  # SSL mode (Render requires SSL)
        )

        if connection:
            logging.info("Connected to PostgreSQL database")
            return connection
        else:
            raise Exception("Failed to connect to the database")

    except Error as e:
        logging.error(f"Error while connecting to PostgreSQL: {e}")
        return f"Error while connecting to PostgreSQL: {e}", 500
    except Exception as e:
        logging.error(f"Error while connecting to PostgreSQL: {e}")
        return f"Error while connecting to PostgreSQL: {e}", 500
