import sys
from optparse import OptionParser
import MySQLdb
import psycopg2
import pandas as pd
__version__ = "1.3.3"

#connect to mysql       
def myconnect(uname, passw, host_name, db_name, tb_name):
    try:
        mcon = MySQLdb.connect(host=host_name, user=uname, passwd=passw, db=db_name)
        try:
            sq_tb = pd.read_sql('select * from '+ ' %s ' % tb_name, mcon)
            df = pd.DataFrame(sq_tb)
            mcon.close()
            db_analyses(df)
        except Exception:
            print("Ensure that table name is correct")
    except Exception:
        print("Ensure connection details re correct.")
        
#connect to postgresql    
def pconnect(uname, passw, host_name, db_name, tb_name):
    try:
        pcon = psycopg2.connect("host ="+"%s" %host_name + " user ="+"%s" %uname + " password ="+"%s" %passw + " dbname ="+"%s" %db_name)
        try:
            ptable = pd.read_sql('select * from '+ ' %s ' % tb_name, pcon)
            df = pd.DataFrame(ptable)
            pcon.close()
            db_analyses(df)
        except Exception:
            print("Ensure table name is correct.")
    except Exception:
        print("Ensure connection details re accurate.")
        
#data analyses
def db_analyses(df):
    print("Statistics: %s" %df.describe())
    print("Head: %s"  %df.head())
    print("Tail: %s"  %df.tail())
    print("Correlation: %s" %df.corr())
    print("Covarriance: %s" %df.cov())
    print("Kurt: %s" %df.kurt())
    print("Skew: %s" %df.skew())
    print("Summation: %s" %df.sum())
    print("Maximum: %s" %df.max())
    print("Minimum: %s" %df.min())
    
if __name__ == '__main__':
    parser = OptionParser("usage%prog -u <username> -p <password> -s <host> -d <database> -t <table name>")                     
    parser.add_option("-u", "--uname", dest="uname", help="USER for db", metavar="UNAME")                 
    parser.add_option("-p", "--passw", dest="passw", help="PASSWORD for db", metavar="PASSW")                 
    parser.add_option("-s", "--host_name", dest="host_name", default="localhost", help="HOST for db", metavar="HOST_NAME")
    parser.add_option("-d", "--db_name", dest="db_name", help="DATABASE name", metavar="DB_NAME")                 
    parser.add_option("-t", "--tb_name", dest="tb_name", help="TABLE name", metavar="TB_NAME")                 
    (options, args) = parser.parse_args()
    if options.uname and options.passw and options.host_name and options.db_name and options.tb_name:
        print('options: %s, args: %s' %(options, args))
        #python3
        #db_options = input("Enter database: mysql, postgres, or quit: ")
        db_options = raw_input("Enter database: mysql, postgres, or quit: ")
        if db_options == "mysql":
            my_connector = myconnect(options.uname, options.passw, options.host_name, options.db_name, options.tb_name)
        elif db_options == "postgres":
            my_pconnector = pconnect(options.uname, options.passw, options.host_name, options.db_name, options.tb_name)
        elif db_options == "quit" or "q":
            sys.exit()
    else:
        print(parser.usage)
        #exit(0)
        
        
        
        
    
    
    
    
    
