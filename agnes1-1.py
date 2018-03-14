# A DB Table Statistics Viewer

import MySQLdb
import psycopg2
from pandas import *
import sys
from PyQt4.QtCore import *
from PyQt4.QtGui import *
__version__ = "1.1.0"

class Agnes(QTabWidget):
    def __init__(self, parent=None):
        super(Agnes, self).__init__(parent)
        self.tab1 = QWidget()
        self.tab2 = QWidget()
        
        self.addTab(self.tab1, "DB Table Statistics")
        self.addTab(self.tab2, "Full-View/Help")
        self.tab1UI()
        self.tab2UI()
        self.setWindowTitle("Agnes DB Statistics")

    def tab1UI(self):
        self.labelhost = QLabel("Host")
        self.host_le = QLineEdit()

        self.label_user = QLabel("User")
        self.user_le = QLineEdit()

        self.label_passwd = QLabel("Passwd")
        self.password_le = QLineEdit()
        self.password_le.setEchoMode(QLineEdit.Password)
        
        self.label_db = QLabel("DB")
        self.db_le = QLineEdit()

        self.browser = QTextBrowser()
        self.browser.setAlignment(Qt.AlignCenter)
        
        self.btn_stat = QPushButton("View DB Table Statistics")
        self.btn_stat.setStyleSheet("color:blue")
    
        self.rbtn = QRadioButton("mysql")
        self.rbtn1 = QRadioButton("postgres")


        flayout = QFormLayout()
        hbox1 = QHBoxLayout()
        hbox1.addWidget(self.labelhost)
        hbox1.addWidget(self.host_le)
        hbox1.addWidget(self.label_user)
        hbox1.addWidget(self.user_le)
        

        hbox2 = QHBoxLayout()
        hbox2.addWidget(self.label_db)
        hbox2.addWidget(self.db_le)
        hbox2.addWidget(self.label_passwd)
        hbox2.addWidget(self.password_le)
        
        hbox3 = QHBoxLayout()
        hbox3.addWidget(self.rbtn)
        hbox3.addWidget(self.rbtn1)

        flayout.addRow(hbox1)
        flayout.addRow(hbox2)
        flayout.addRow(hbox3)
        flayout.addRow(self.btn_stat)
        flayout.addRow(self.browser)
    
        self.tab1.setLayout(flayout)
        self.setWindowTitle("Agnes DB Statistics")
        self.setGeometry(5, 5, 900, 500)
        
        self.btn_stat.clicked.connect(self.updateUI)

        self.connect(self.rbtn,
                     SIGNAL("Toggled()"), self.updateUI)
        self.connect(self.rbtn1,
                     SIGNAL("Toggled()"), self.updateUI)
        

    def tab2UI(self):
        vbox1 = QVBoxLayout()
        self.btn_about = QPushButton("About Agnes")
        self.browser1 = QTextBrowser()
        self.browser1.setAlignment(Qt.AlignCenter)
        vbox1.addWidget(self.btn_about)
        vbox1.addWidget(self.browser1)
        self.tab2.setLayout(vbox1)
        self.btn_about.clicked.connect(self.appInfo)

    def updateUI(self):
        if self.host_le.text() and self.user_le.text() and self.password_le.text() and self.db_le.text():
            if self.rbtn.isChecked():
                self.myconnect()
            elif self.rbtn1.isChecked():
                self.pconnect()
            else:
                self.browser.setText("[*] Please specify DB for connection.")
        else:
            self.browser.setText("[*] Please insert connection details and try again.")

    def myconnect(self):
        user_text1 = str(self.host_le.text())
        user_text2 = str(self.user_le.text())
        user_text3 = str(self.password_le.text())
        user_text4 = str(self.db_le.text())

        try:
            mcon = MySQLdb.connect(host=user_text1, user=user_text2, passwd=user_text3, db=user_text4)
            self.browser.setText("[*] Welcome, connection successful.")
            text, ok = QInputDialog.getText(self, "Table Name", "Enter table name:")
            if ok and text:
                tb_name = str(text)
                try:
                    sq_tb = pandas.read_sql('select * from '+ ' %s ' % tb_name, mcon)
                    df = pandas.DataFrame(sq_tb)
                    mcon.close()
                    size = str(len(df))
                    stat_description = df.describe()
                    stats = str(stat_description)
                    kt = str(df.kurt())
                    skew = str(df.skew())
                    cov = str(df.cov())
                    corr = str(df.corr())
                    head = str(df.head())
                    tail = str(df.tail())
                    summation = str(stat_description.sum())
                    self.browser1.setText("Size: " +"%s " %size +"\n"\
                                          +"Statistics:" +"\n"\
                                          +" %s " %stats +"\n"\
                                          +"Kurt:" +"\n"\
                                          +"%s" %kt +"\n"\
                                          +"Skew:" +"\n"\
                                          +"%s" %skew +"\n"\
                                          +"Covarriance:" +"\n"\
                                          +"%s" %cov +"\n"\
                                          +"Correlation:" +"\n"\
                                          +"%s" %corr +"\n"\
                                          +"Summation:" +"\n"\
                                          +"%s" %summation +"\n"\
                                          +"Head:" +"\n"\
                                          +"%s" % head +"\n"\
                                          +"Tail:" +"\n"\
                                          +"%s" %tail)
                    self.browser.setText(stats)
                    self.host_le.clear()
                    self.user_le.clear()
                    self.password_le.clear()
                    self.db_le.clear()
                    
                except Exception, e:
                    self.browser.setText("[*] Ensure that the table name is correct and try again.")
        except Exception, e:
            self.browser.setText("Please specify correct connection details and try again")

    def pconnect(self):
        user_text1 = str(self.host_le.text())
        user_text2 = str(self.user_le.text())
        user_text3 = str(self.password_le.text())
        user_text4 = str(self.db_le.text())            
        try:
            pcon = psycopg2.connect("host ="+"%s" %user_text1 + " user ="+"%s" %user_text2 + " password ="+"%s" %user_text3 + " dbname ="+"%s" %user_text4)
            self.browser.setText("[*] Welcome, connection successful.")
            text, ok = QInputDialog.getText(self, "Table Name", "Enter table name:")
            if ok and text:
                tb_name = str(text)
                try:
                    ptable = pandas.read_sql('select * from '+ ' %s ' % tb_name, pcon)
                    df = pandas.DataFrame(ptable)
                    pcon.close()
                    size = str(len(df))
                    stat_description = df.describe()
                    stats = str(stat_description)
                    kt = str(df.kurt())
                    skew = str(df.skew())
                    cov = str(df.cov())
                    corr = str(df.corr())
                    head = str(df.head())
                    tail = str(df.tail())
                    summation = str(stat_description.sum())
                    self.browser1.setText("Size: "+"%s" %size +"\n"\
                                          +"Statistics:" +"\n"\
                                          +" %s " %stats +"\n"\
                                          +"Kurt:" +"\n"\
                                          +"%s" %kt +"\n"\
                                          +"Skew:" +"\n"\
                                          +"%s" %skew +"\n"\
                                          +"Covarriance:" +"\n"\
                                          +"%s" %cov +"\n"\
                                          +"Correlation:" +"\n"\
                                          +"%s" %corr +"\n"\
                                          +"Summation:" +"\n"\
                                          +"%s" %summation +"\n"\
                                          +"Head:" +"\n"\
                                          +"%s" %head +"\n"\
                                          +"Tail:" +"\n"\
                                          +"%s" %tail)
                    self.browser.setText(stats)
                    self.host_le.clear()
                    self.user_le.clear()
                    self.password_le.clear()
                    self.db_le.clear()
                    
                except Exception, e:
                    self.browser.setText("[*] Ensure that the table name is correct and try again.")
        except Exception, e:
            self.browser.setText("[*] Please specify correct connection details and try again.")


    def appInfo(self):
        dlg = QMessageBox()
        dlg.setIcon(QMessageBox.Information)
        dlg.setInformativeText("This application is written in Python 2.7.14+ and PyQt4." +"\n"\
                               "Version: 1.1.0" +"\n"\
                               "Copyright (C) 2018 Daniel Osinachi N."+"\n"\
                               "dan.ossy.do@gmail.com")
        dlg.setDetailedText("Agnes is a one click view of your DB table statistics."+"\n"\
                            "Version: 1.1.0")
        dlg.setStandardButtons(QMessageBox.Ok)
        info = dlg.exec_()
        if info == QMessageBox.Ok:
            pass
                                                   
app = QApplication(sys.argv)
form = Agnes()
form.show()
app.exec_()


        
        
        


    
    
