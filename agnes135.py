import sys
from optparse import OptionParser
from sqlalchemy import create_engine
import pandas as pd
__version__ = "1.3.5"

#connect to mysql       
def myconnect(uname, passw, host_name, db_name, tb_name):
    try:
        mcon = create_engine("mysql+mysqldb://"+"%s"%uname+":"+"%s"%passw+"@"+"%s"%host_name+"/"+"%s"%db_name)
        con1 = mcon.connect()
        sq_tb = pd.read_sql('%s' %tb_name, con1)
        df1 = pd.DataFrame(sq_tb)
        db_analyses(df1)
        con1.close()
    except Exception:
        print("Ensure connection details re correct.")
    
        
#connect to postgresql    
def pconnect(uname, passw, host_name, db_name, tb_name):
    try:
        pcon = create_engine("postgresql://"+"%s"%uname+":"+"%s"%passw+"@"+"%s"%host_name+":5432/"+"%s"%db_name)
        con2 = pcon.connect()
        ptable = pd.read_sql('select * from '+ ' %s ' % tb_name, con2)
        df2 = pd.DataFrame(ptable)
        db_analyses(df2)
        con2.close()
    except Exception:
        print("Ensure connection details re accurate.")
        
#connect to oracle
def oconnector(uname, passw, host_name, db_name, tb_name):
    try:
        ocon = create_engine("oracle://"+"%s"%uname+":"+"%s"%passw+"@"+"%s"%host_name+":1521/"+"%s"%db_name)
        con3 = ocon.connect()
        otable = pd.read_sql("%s" %tb_name, con3)
        df3 = pd.DataFrame(otable)
        db_analyses(df3)
        con3.close()
    except Exception:
        print("Ensure connection details re accurate.")

#connect to mssql
def mss_connector(db_name, tb_name):
    try:
        mss = create_engine("mssql+pyodbc://"+"%s"%db_name)
        con4 = mss.connect()
        mss_table = pd.read_sql("%s" %tb_name, con4)
        df4 = pd.DataFrame(mss_table)
        db_analyses(df4)
        con4.close()
    except Exception:
        print("Ensure connection details re accurate.")

#connect to sqlite
def mylite(db_name, tb_name):
    try:
        slite = create_engine("sqlite:///"+"%s"%db_name+".db")
        con5 = slite.connect()
        lite_tb = pd.read_sql("%s" %tb_name, con5)
        df5 = pd.DataFrame(lite_tb)
        db_analyses(df5)
        con5.close()
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
    parser = OptionParser("usage%prog -u <username> -p <password> -s <host> -d <db name or sidname> -t <table name>")
    parser1 = OptionParser("useage%prog -d <db name or mydsn> -t<table name>")
    parser.add_option("-u", "--uname", dest="uname", help="USER for db", metavar="UNAME")                 
    parser.add_option("-p", "--passw", dest="passw", help="PASSWORD for db", metavar="PASSW")                 
    parser.add_option("-s", "--host_name", dest="host_name", default="localhost", help="HOST for db", metavar="HOST_NAME")
    parser.add_option("-d", "--db_name", dest="db_name", help="DATABASE name", metavar="DB_NAME")                 
    parser.add_option("-t", "--tb_name", dest="tb_name", help="TABLE name", metavar="TB_NAME")                 
    (options, args) = parser.parse_args()

    if options.uname and options.passw and options.host_name and options.db_name and options.tb_name:
        #python3
        #db_options = input("Enter database: mysql, postgres, or quit: ")
        db_options = raw_input("Enter database: mysql, postgres, oracle, or quit: ")
        if db_options == "mysql":
            my_connector = myconnect(options.uname, options.passw, options.host_name, options.db_name, options.tb_name)
        elif db_options == "postgres":
            my_pconnector = pconnect(options.uname, options.passw, options.host_name, options.db_name, options.tb_name)
        elif db_options == "oracle":
            oconnector(options.uname, options.passw, options.host_name, options.db_name, options.tb_name)
        elif db_options == "quit" or "q":
            sys.exit()
    elif options.db_name and options.tb_name:
        db_options1 = raw_input("Enter database: mssql, sqlite or quit: ")
        if db_options1 == "mssql":
            mss_connector(options.db_name, options.tb_name)
        elif db_options1 == "sqlite":
            mylite(options.db_name, options.tb_name)
        elif db_options1 == "quit" or "q":
            sys.exit()
            
    else:
        print(parser.usage)
        print("OR")
        print(parser1.usage)
