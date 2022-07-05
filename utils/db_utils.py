import psycopg2
import psycopg2.extras as extras

from configs import settings

def get_db_connection():
	conn, cur = None, None
	try:
		print('connecting to database')
		conn = psycopg2.connect(
			database=settings.DATABASE,
			user=settings.USER,
			password=settings.PASSWORD, 
			host=settings.HOST,
			port=settings.PORT
		)
		cur = conn.cursor()
		return conn, cur

	except Exception as error:
		print(error)

	
	
