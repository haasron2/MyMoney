import sys
import json
import tkinter as tk
from tkinter import *
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText
from tkinter import filedialog as fd
import tkinter.font as font

sys.path.append('c:\\users\\haasr\\appdata\\local\\packages\\pythonsoftwarefoundation.python.3.10_qbz5n2kfra8p0\\localcache\\local-packages\python310\\site-packages')
import sqlite3
import xlsxwriter
import pandas as pd
#pd.options.mode.use_inf_as_na = True
import datetime
from datetime import date,timedelta
from dateutil.relativedelta import relativedelta
from tkcalendar import Calendar

from sqlite3 import Error
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from bidi import algorithm as bidialg
from scipy.stats import gaussian_kde
from os import listdir
from shutil import copyfile
import numpy as np
from tkinter import messagebox
import codecs
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn import ensemble
from sklearn import linear_model
from sklearn.metrics import mean_absolute_error
from sklearn.compose import ColumnTransformer
from scipy.stats import randint
from sklearn.model_selection import RandomizedSearchCV
from sklearn.metrics import mean_squared_error, r2_score,mean_absolute_error,mean_squared_log_error
from sklearn import svm
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from pylint import pyreverse
from sklearn.model_selection import GridSearchCV
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV
import joblib

pd.set_option('display.max_columns', None)



##from bidi import algorithm as bidialg
##import matplotlib.pyplot as plt
##text = bidialg.get_display(u'שלום כיתה א')
##plt.text(0.5, 0.5, text , name = 'Arial')
##plt.show()

def iniialise_param_json (json_param_file):
   global param_json
   f = open(json_param_file, "r")
   param_json = json.loads(f.read())    

def create_connection(path):
    connection = None
    try:
        connection = sqlite3.connect(path)
        print("Connection to SQLite DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection



def execute_sql(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")

def execute_sql_no_except(connection, query,args = None):

    cursor = connection.cursor()
    result = None
    if args == None :
      cursor.execute(query)
    else:
      cursor.execute(query,args)
      
    result = cursor.fetchall()
    return result,cursor.description




# this class handles all the work related to query from executing , rerieve data,retriev column   etc
#______________________________________________________________________________________
class MyQuery :
    def __init__(self, dbConnection,query_text,args):
           
            self.dbConnection = dbConnection
            self.query_text = query_text
            self.query_args = args
#            self.result_query,self.cursor_desc = execute_sql_no_except(dbConnection,query_text,args)     
            self.query_simple_result = None
            self.query_pandas_result_df = None
            self.query_simple_desc = None

#            self.execute_simple_query()

    def set_query_text(self,query_text):
        self.query_text = query_text

    def get_query_result(self):
        return self.result_query

    def get_query_desc(self):
        return self.query_desc

    def get_query_text(self):
        return self.query_text

    def execute_simple_query(self) :
        cursor = self.dbConnection.cursor()
        result = None
        if self.query_args == None :
          cursor.execute(self.query_text)
        else:
          cursor.execute(self.query_text,self.query_args)
          
        self.query_simple_result = cursor.fetchall()
        self.query_simple_desc = cursor.description
        #print (cursor.description)
        #return result,cursor.description        

    def execute_pandas_query(self):
        cursor = self.dbConnection.cursor()   
        if self.query_args == None :
          self.query_pandas_result_df = pd.read_sql_query(self.query_text,self.dbConnection)
        else:
          # print(self.query_args)
          self.query_pandas_result_df = pd.read_sql_query(self.query_text,self.dbConnection,params=self.query_args)

        #return self.query_pandas_result_df

    def get_result_coulmns_list(self):
        result = list()
        for col_name in self.query_pandas_result_df.columns:
            #print (col_name)
            result.append(col_name)

        return result

    def get_result_column_count(self,column_name):
        return self.query_pandas_result_df[column_name].count()

    def get_result_column_sum(self,column_name):
        return self.query_pandas_result_df[column_name].sum()

    def get_result_column_min(self,column_name):
        return self.query_pandas_result_df[column_name].min()

    def get_result_column_max(self,column_name):
        return self.query_pandas_result_df[column_name].max()

##---------------------------------------------------------------------
##                 ApplQuery
##---------------------------------------------------------------------
# this class handle the query store its text , column , params ..
# it has query_id m df-dic which holds all the values like name , category , type and query text
# it has data fra,e for ots columns , fpr its params , for its resu;t and other variables
# for its operatipns
class  ApplQuery ( ):
    def __init__(self, dbConnection,query_id,args):
           self.dbConnection = dbConnection
           self.query_id = query_id
           self.df_dic = None
           self.dic_properties = None
           self.df_columns = None
           self.df_params = None
           self.df_result = None
           self.load_query_metadata(query_id)
           #self.detail_queries = list()
           #self.detail_queries_names = list()
           self.detail_queries_df =  pd.DataFrame(columns=['A','B'])
           self.args = args
           # this is for the applquery to sAVE THE LAST ROW NUMBER  before we exit to other applquert
           #self.last_dbtreeview_row = None  
           self.last_tree_view_sort = None
           self.last_tree_view_column_name_sort = None

    def load_query_metadata (self,query_id) :          
           # query data from appl_query and populate dict with data and super with query text
           mq1 = MyQuery(self.dbConnection,"select * from appl_query where query_id = ?",[query_id])
           mq1.execute_pandas_query()
           self.df_dic = mq1.query_pandas_result_df
           # construct dicitionary with all the appl_query properties
           self.dic_properties = self.df_dic.to_dict()
           #super().__init__(dbConnection,self.dic_properties["query_text"][0],args)

##           if result_y_n == 'Y' :
##             mq4 = MyQuery(self.dbConnection,self.dic_properties['query_text'][0],None)
##             mq4.execute_pandas_query()
##             self.df_result = mq4.query_pandas_result_df
##             self.update_query_columns()

           mq2 = MyQuery(self.dbConnection,"select * from appl_query_columns where query_id = ?",[query_id])
           mq2.execute_pandas_query()
           self.df_columns = mq2.query_pandas_result_df

           mq3 = MyQuery(self.dbConnection,"select * from appl_query_params where query_id = ?",[query_id])
           mq3.execute_pandas_query()
           self.df_params = mq3.query_pandas_result_df


    # this function runs the query and populate the data frane result

    def run_query_data (self) :
            
      # check tp see if it is  a procedure or sql 
       if self.get_query_type() == 'procedure':
          #print('it is a procedure')

          df_exec = pd.DataFrame({  'C': [1, 2, 3],  'D': [4, 5, 6] })
          #df_result =  pd.DataFrame()
          #mq1 = MyQuery(self.dbConnection,None,None)
          #print(self.get_query_text())
          #exec(self.get_query_text(),{'dbConnection': mydbcon,'pd':pd,'df_result':df_result})
          #exec(self.get_query_text(),{'df_exec':df_exec})
          locals = {}
          # exec the procedure - the output from the procedure is in the locals which is df_exec result
          # the procedure must return this data frame result before its end
          exec(self.get_query_text(), globals(), locals)
          self.df_result=locals ['df_exec']
          # it is an sql 
       else :
                       # if there are no args
           if  self.args is None :
              mq4 = MyQuery(self.dbConnection,self.dic_properties['query_text'][0],None)
              mq4.execute_pandas_query()
            # there are args for the query 
           else :
              mq4 = MyQuery(self.dbConnection,self.dic_properties['query_text'][0],self.args)
              mq4.execute_pandas_query()              
           # populate data frame reult   
           self.df_result = mq4.query_pandas_result_df

        # update counter of usage for this appl query
       args = list()
       args.append(self.query_id)
       execute_sql_no_except(mydbcon,"update appl_query set query_usage = query_usage + 1 where query_id = ? ",args )           
       execute_sql_no_except(mydbcon, "commit")
           
    def get_query_id (self):
        return self.dic_properties["query_id"][0]
      
    def get_query_text (self):
        return self.dic_properties["query_text"][0]

    def get_query_name (self):
        return self.dic_properties["query_name"][0]

    def get_query_type (self):
        return self.dic_properties["query_type"][0]
    def get_query_text (self):
        return self.dic_properties["query_text"][0]

    def update_query_columns(self):
        args_del = list()
        args_del.append(self.query_id)
        execute_sql_no_except(mydbcon, "delete from  appl_query_columns   where query_id = ?",args_del)
        
        result = list()
        for i,col_name in enumerate(self.df_result.columns):
            #print (col_name)
            result.append(col_name)
            
            args_col = (self.query_id,i+1,col_name)
            execute_sql_no_except(mydbcon, "insert into   appl_query_columns (query_id,column_seq,column_name) values(?,?,?)",args_col)
            execute_sql_no_except(mydbcon, "commit")

    def get_query_params (self):
        args_del = list()
        args_del.append(self.query_id)       
        result_query,cursor_desc1= execute_sql_no_except(mydbcon,"select param_name,param_value where query_id = ? ",args)
        

    def get_query_args (self):
        return  self.args      

            
    def set_query_text (self,query_text):
        args= (query_text,self.query_id)
        #args.append(query_text,self.query_id)
        execute_sql_no_except(mydbcon, "update  appl_query  set query_text = ? where query_id = ?",args)
        execute_sql_no_except(mydbcon, "commit")
        self.dic_properties['query_text'] = {0:query_text}
        #print(self.dic_properties['query_text'])

    def set_query_name (self,query_name):
        args= (query_name,self.query_id)
        #args.append(query_text,self.query_id)
        execute_sql_no_except(mydbcon, "update  appl_query  set query_name = ? where query_id = ?",args)
        execute_sql_no_except(mydbcon, "commit")
        self.dic_properties['query_name'] = query_name

    def set_query_type (self,query_type):
        args= (query_type,self.query_id)
        #args.append(query_text,self.query_id)
        execute_sql_no_except(mydbcon, "update  appl_query  set query_type = ? where query_id = ?",args)
        execute_sql_no_except(mydbcon, "commit")
        self.dic_properties['query_type'] = query_type

    def set_query_category (self,query_category):
        args= (query_category,self.query_id)
        #args.append(query_text,self.query_id)
        execute_sql_no_except(mydbcon, "update  appl_query  set query_category = ? where query_id = ?",args)
        execute_sql_no_except(mydbcon, "commit")
        self.dic_properties['query_category'] = {0:query_category}

        
 # this function finds the matches appl query details for this app query
 # and popluate the list of self.detail_queries with the names
    def find_detail_queries(self):
      
       #self.detail_queries = list()
       # self.detail_queries_names = list()

       # we declare an empty data frame with columns 
        self.detail_queries_df = pd.DataFrame(columns=['query_id','query_name','query_usage','input_param_count','input_param_count_distinct'])
        
        # we want to check it the detail query has the exact input params as in the query columns
        sql_text = "select aqp.query_id,query_name,query_usage,count( distinct param_value) count_params  ,count( distinct param_value) count_params_distinct \
                     from appl_query_params aqp  join appl_query aq  \
                     on (aqp.query_id = aq.query_id)   \
                     where param_name like 'input_param%'   \
                     group by aqp.query_id ,query_name,query_usage  "
        
        result_query,cursor_desc1=execute_sql_no_except(mydbcon,sql_text,None)
        for res in result_query :

            args =(res[0],self.query_id)

            result_query,cursor_desc1=execute_sql_no_except(mydbcon,"select count(*) from (select   param_value from appl_query_params where param_name like 'input_param%' \
                                and query_id = ? intersect select column_name from appl_query_columns where query_id = ? ) ",args)
           # print(result_query[0][0] ,res[3])
            if result_query[0][0] == res[4] :
                # we create the list of values of the next row 
               new_row_list = [res[0],res[1],res[2],res[3],res[4]]
                 # append it at the end of the dta frame 
               self.detail_queries_df.loc[len(self.detail_queries_df)] = new_row_list
               #print(self.detail_queries_df)
        # we sort the data frame accordint to sumber of params that matches the query and the usage of the detal query 
        self.detail_queries_df = self.detail_queries_df.sort_values(['input_param_count', 'query_usage'], ascending=[False, False])
       # print(self.detail_queries_df)

        # at the end we get data frame like this  where we can use later to show the detail queries
               ##        query_id                       query_name  query_usage  input_param_count
               ##3         59    det_trans_by_date_and_account           24                  2
               ##1         52                  det_label_trans           56                  1
               ##0         51                   det_trans_desc           30                  1
               ##7         70           det_label_sum_by_month           18                  1
               ##4         67  det_labels_hirachy_sum_by_month           17                  1
               ##5         68      det_labels_hirarchy_by_year           16                  1
               ##6         69         det_label_hirarchy_trans           13                  1
               ##10       111             det_label_todo_trans            9                  1
               ##2         55                   det_trans_date            8                  1
               ##8         71            det_label_sum_by_year            8                  1
               ##9         88            det_trans_ref_acnt_id            4                  1

#    this function gets param_name string and return all the vlaues
# which matches the string sorted by the param_name 
    def get_values_list_for_param(self,param_string):
      selected_rows=self.df_params[self.df_params['param_name'].str.contains(param_string)]
      sorted_selected_rows = selected_rows.sort_values(by='param_name')
      param_values = sorted_selected_rows['param_value'].tolist()

      return param_values


#    this function gets param_name string and return all the param_names
# which matches the string sorted by the param_name 
    def get_param_name_list_for_param(self,param_string):
      selected_rows=self.df_params[self.df_params['param_name'].str.contains(param_string)]
      sorted_selected_rows = selected_rows.sort_values(by='param_name')
      param_names = sorted_selected_rows['param_name'].tolist()
      return param_names



#    this function gets param_values history  list  for dialog params from table appl_query_args_history
# which matches the string sorted by the param_name 
    def get_args_history_list_for_param(self,param_string):
       sql_args = [self.query_id,'dialog%']
       result_query,cursor_desc1 = execute_sql_no_except(mydbcon, "select arg_value from    appl_query_args_history   \
             where  query_id = ? and param_name like ?  order by param_name",sql_args)

       arg_history_list = list ()
       for res in result_query :
          #print(res)
          arg_history_list.append(res[0])
         

       return arg_history_list



# this function return the number of args needed in the query text
    def get_args_number_in_query_text (self):
        query_text = self.get_query_text()
        return query_text.count('?')
      
        
    @staticmethod
    def new_appl_query(self):
        # we get the max query id number
        result_query,cursor_desc1 = execute_sql_no_except(mydbcon, "select max(query_id) from appl_query")
        query_id = int(result_query[0][0])+1
        # we add the number +1 to build the new query name 
        new_query_name = "new query name " + str(query_id)
        args = (new_query_name,"select count(*) from labels","primary","stage")
        execute_sql_no_except(mydbcon, "insert into  appl_query(query_name,query_text,query_type,query_category,query_usage) values(?,?,?,?,0)",args )
        execute_sql_no_except(mydbcon, "commit")
        return ApplQuery(mydbcon,query_id,None)

    def open_appl_query(self,query_name):
        # we get the max query id number
        args = list()
        args.append(query_name)
        result_query,cursor_desc1 = execute_sql_no_except(mydbcon, "select query_id from appl_query where query_name = ?",args)
        query_id = int(result_query[0][0])
        # we add the number +1 to build the new query name 
        query_id = int(query_id)
        return ApplQuery(mydbcon,query_id,None)
    
    def get_query_name_stat(self,query_id) :
          args = list()
          args.append(query_id)
          result_query,cursor_desc1 = execute_sql_no_except(mydbcon, "select query_name from appl_query where query_id = ?",args)
          #print (result_query[0][0])
          return result_query[0][0]


    def get_query_id_stat(self,query_name) :
          args = list()
          args.append(query_name)
          result_query,cursor_desc1 = execute_sql_no_except(mydbcon, "select query_id from appl_query where query_name = ?",args)
          #print (result_query[0][0])
          return result_query[0][0]

    def get_query_type_stat(self,query_name) :
          args = list()
          args.append(query_name)
          result_query,cursor_desc1 = execute_sql_no_except(mydbcon, "select query_type from appl_query where query_name = ?",args)
          #print (result_query[0][0])
          return result_query[0][0]

    def get_unique_column_values (self,column_name ):
        unique_col_values = self.df_result[column_name].unique()
        return unique_col_values
    
##---------------------------------------------------------------------
##                 dbTreeView
##---------------------------------------------------------------------
# this class handle the dbtree view - load from data frame , clear        


class dbTreeView (ttk.Treeview):
    def __init__(self,frame,columns,height):
        style = ttk.Style()
        style.theme_use('clam')
        super().__init__(frame,columns=columns,height = height)
     #    ttk.Treeview(self, columns=columns, show='headings')
#        for head_text in columns :
#              self.heading(head_text,text=head_text)
        
    def clear_all_items(self):
        # clear all the items in the treview 
        for item in self.get_children():
           self.delete(item)

  # this function popu;tae the treeview
  # if it gets color column value - it handle it with the proper red or green colors
  # if it gets width dictionary for columns
  # it poplates the tree with the requires width of each column
  
    def populate_from_data_frame( self,df,color_column=None,columns_width_dictionary = None,hirarchy_column = None):
        # populate the header
        self.column("#0", width=0, anchor=CENTER,stretch = "no")
        # set the columns for the tree view
        tree_columns = list()
        for idx ,head_text in enumerate(df.columns) :
          #cursor_head_str= cursor_head_str +head_text[0].ljust(20)
          tree_columns.append(head_text)        

        # define the columns for the tre view 
        self['columns'] = tree_columns

        # check  we required to check column width
        # the dictionay contains for each column the width for example
        # {'trans_desc': 200, 'default': 80}
        # default is the defua;t width for all the other columns
        
        if columns_width_dictionary is None :        
          for idx,col_name in enumerate(df.columns):         
        #    for header in tree_columns:
            self.heading(idx, text=col_name,anchor=CENTER)
            self.column(idx, minwidth=0,width=80, anchor=CENTER, stretch=NO)
        else :
           # we need to check width of each column
           # first thing is we get the dict
           #print(columns_width_dictionary)
           for idx,col_name in enumerate(df.columns):
             #self.heading(idx, text=col_name,anchor=CENTER)
             self.heading(idx, text=col_name)
             if col_name in columns_width_dictionary  :
                #print(columns_width_dictionary[col_name])
                self.column(idx, minwidth=0,width=columns_width_dictionary[col_name], anchor=CENTER, stretch=NO)
             else:
                # we didnt get width for this column s owe set it to defualt 80
                self.column(idx, minwidth=0,width=columns_width_dictionary['default'], anchor=CENTER, stretch=NO)
  
               
        # populate the row in the tree view

        #hirarchy_column =  None    #'level'

        if hirarchy_column is not None: 
            print ('hi')
            #this initialise 0 in 9 levels of hirarchy 
            parent_level_number = list()
            parent_level_counter = list()
            parent_level_sum_1 = list()
            parent_level_sum_2 = list()
            # initialise it with 10 zeros
            parent_level_number = [0] * 10
            parent_level_counter = [0] * 10
            parent_level_sum_1 = [0] * 10
            parent_level_sum_2 = [0] * 10

 
            # iterate over the rows 
            for idx,row in df.iterrows():
               tup_list = list()
               for item in df.columns:
                  tup_list.append(row[item])

            # get the hirachy level of this row                        
                #hirarchy_row_level = int(tup_list[int(hirarchy_column)])
              # print(tup_list)
              # print(tree_columns)
               index = tree_columns.index(hirarchy_column)
               hirarchy_row_level = int(tup_list[index])
               # this will control the color - biger level - darker color - with hirarchy tag pre configured
               if hirarchy_row_level == 0 :
                  hirarchy_color_tag = 'hirarchy'
               else :
                  hirarchy_color_tag = 'hirarchy'+str(hirarchy_row_level)
               
               if hirarchy_row_level == 0 :
                  parent_level_number[0] = idx
                  parent_level_counter[0] = 0
                 # self.result_dbTreeView.insert('', tk.END,id=idx ,values=tup_list,tag = ('hirarchy'))
                  self.insert('', tk.END,id=idx ,values=tup_list,tag = (hirarchy_color_tag))
               else :
                  #self.result_dbTreeView.insert('', tk.END,id=idx ,values=tup_list)
                  self.insert('', tk.END,id=idx ,values=tup_list,tag = (hirarchy_color_tag))
                  parent_level_number[hirarchy_row_level] = idx
                  parent_level_counter[hirarchy_row_level-1] =  parent_level_counter[hirarchy_row_level-1] +1 
                  parent_level_counter[hirarchy_row_level] = 0
                 #  self.result_dbTreeView.move(idx, parent_level_number[hirarchy_row_level-1], parent_level_counter[hirarchy_row_level-1])
                  self.move(idx, parent_level_number[hirarchy_row_level-1], parent_level_counter[hirarchy_row_level-1])

            
        elif  color_column == None :
           for idx,row in df.iterrows():
               tup_list = list()
               for item in df.columns:
                  tup_list.append(row[item])
                  
               self.insert('', tk.END,id=idx ,values=tup_list)
        else :
           for idx,row in df.iterrows():
               tup_list = list()
               for i,item in enumerate(df.columns):
                  tup_list.append(row[item])
                  if item == color_column :
                     color_column_idx = i

               #print(tup_list)                  
               if float(tup_list[color_column_idx]) < 0 :
                 self.insert('', tk.END,id=idx ,values=tup_list,tag = ('expens'))
               else  :
                 self.insert('', tk.END,id=idx ,values=tup_list,tag = ('income'))
         
            

##---------------------------------------------------------------------
##                 QueryGui
##---------------------------------------------------------------------
# this class is the query window to edit , save , execure ,  create ..                  


class QueryGui(tk.Tk):    
  
  def __init__(self,query_id):
    super().__init__()
    #self.master = master
    self.title("Query Gui ")
    self.geometry("1800x1000")

    global f_labels
    f_labels= font.Font(family=None, size=12, weight="bold")
    global f_button
    f_button= font.Font(family=None, size=12)
    global f_data
    f_data = font.Font(family=None, size=12, weight="normal")
    global f_entry
    f_entry = font.Font(family=None, size=12, weight="normal")
    global f_combobox
    f_combobox = font.Font(family=None, size=12, weight="normal")
   
    self.applQuery1 = None
    self.query_name_return = None
    # this are lists to hold the last state of the query before it exit to detail query 
    self.query_history_list = list()
    self.dbtreeview_row_history_list = list()  
    self.treeview_sort_history_list = list()
    # this flag is set to asc another click will switch to desc
    self.sort_asc_desc ='asc'
    
   # query header create elements
    query_header_frame = Frame(self)
    query_id_label = Label(query_header_frame,text="Query ID",justify=LEFT,font=f_labels)
    query_name_label = Label(query_header_frame,text="Query name",justify=LEFT,font=f_labels)
    query_type_label = Label(query_header_frame,text="Query Type",justify=LEFT,font=f_labels)
    query_category_label = Label(query_header_frame,text="Query category",justify=LEFT,font=f_labels)
    query_counter_label = Label(query_header_frame,text="Query Usage",justify=LEFT,font=f_labels)
    self.query_id_entry = Entry(query_header_frame,font=f_entry)
    self.query_name_entry = Entry(query_header_frame,font=f_entry)
    #self.query_type_entry = Entry(query_header_frame)
    #self.query_category_entry = Entry(query_header_frame)
    self.query_type_combobox = ttk.Combobox(query_header_frame, textvariable=None,font=f_combobox)
    self.query_category_combobox = ttk.Combobox(query_header_frame, textvariable=None,font=f_combobox)
    self.query_usage_entry = Entry(query_header_frame,font=f_entry)

    # fill the combobox values from query type 
    mq1 = MyQuery(mydbcon,"select query_type from appl_query_type",{})
    mq1.execute_pandas_query()
    df_result = mq1.query_pandas_result_df
    result_list = df_result['query_type'].tolist()
    self.query_type_combobox['values']=result_list

    # fill the combobox values from query category 
    mq1 = MyQuery(mydbcon,"select category_name from appl_query_categories",{})
    mq1.execute_pandas_query()
    df_result = mq1.query_pandas_result_df
    result_list = df_result['category_name'].tolist()
    self.query_category_combobox['values']=result_list


   # query header grid elemnts 

    query_id_label.grid(column=0, row=0,sticky=tk.W)
    query_name_label.grid(column=1, row=0,sticky=tk.W)
    query_type_label.grid(column=2, row=0,sticky=tk.W)
    query_category_label.grid(column=3, row=0,sticky=tk.W)
    query_counter_label.grid(column=4, row=0,sticky=tk.W)

    self.query_id_entry.grid(column=0, row=1,sticky=tk.W)
    self.query_name_entry.grid(column=1, row=1,sticky=tk.W)
    self.query_type_combobox.grid(column=2, row=1,sticky=tk.W)
    self.query_category_combobox.grid(column=3, row=1,sticky=tk.W)
    self.query_usage_entry.grid(column=4, row=1,sticky=tk.W)

   # query frame create elements
    
    query_frame = Frame(self)
    query_label= Label(query_frame,text="Query text",justify=LEFT)
    self.query_scrolled_text = ScrolledText(query_frame,height=18,width=100)
   # query frame grid elemnts     
    query_label.grid(column=0, row=0, sticky=tk.W, padx=5, pady=5)
    self.query_scrolled_text.grid(column=0, row=1, sticky=tk.W, padx=5, pady=5)


   # buttons frame create elements    
    buttons_query_frame = Frame(self,width=25)
    open_query_button = Button(buttons_query_frame,text="OPEN ",command=self.open_choose_query_window,font=f_button)
    exe_query_button = Button(buttons_query_frame,text="EXECUTE ",command=self.execute_query,font=f_button)
    self.back_query_button = Button(buttons_query_frame,text="BACK  ",command=self.back_query,font=f_button)
    #exe_sql_button = Button(buttons_query_frame,text="Execute_SQL",command= self.open_choose_query_window)
    #detail_button = Button(buttons_query_frame,text="Detail_SQL",command=self.open_detail_window)
    button_separator_1 = ttk.Separator(buttons_query_frame,orient='horizontal')
    new_query_button = Button(buttons_query_frame,text="NEW QUERY",command=self.new_query)
    save_query_button = Button(buttons_query_frame,text="SAVE QUERY",command=self.save_query)
    self.open_query_args_button = Button(buttons_query_frame,text="EDIT ARGS",command=self.open_args_window )
    open_query_param_button = Button(buttons_query_frame,text="EDIT Params",command=self.show_query_params)
    button_separator_2 = ttk.Separator(buttons_query_frame,orient='horizontal')    
    load_button = Button(buttons_query_frame,text="LOAD  ",command=self.open_load_window,font=f_button)
    backup_button = Button(buttons_query_frame,text="BACKUP  ",command=self.exp_backup)

  # buttons frame grid  elements     
    open_query_button.grid(column=0, row=0,pady=4,sticky=tk.W)
    exe_query_button.grid(column=0, row=1,pady=4,sticky=tk.W)
    self.back_query_button.grid(column=0, row=2,pady=4,sticky=tk.W)
    button_separator_1.grid(column=0, row=3,pady=4,sticky='ew' )
    #exe_sql_button.grid(column=0, row=2,pady=4,sticky=tk.W)
    #plot_button.grid(column=0, row=3,sticky=tk.W)
    #detail_button.grid(column=0, row=4,sticky=tk.W)
    new_query_button.grid(column=0, row=4,pady=4,sticky=tk.W)
    save_query_button.grid(column=0, row=5,pady=4,sticky=tk.W)
    self.open_query_args_button.grid(column=0, row=6,pady=4,sticky=tk.W)
    open_query_param_button.grid(column=0, row=7,pady=4,sticky=tk.W)
    button_separator_2.grid(column=0, row=8,pady=4,sticky='ew')    
    load_button.grid(column=0, row=9,pady=4,sticky=tk.W)
    backup_button.grid(column=0, row=10,pady=4,sticky=tk.W)
    
  # result frame create   elements    
    result_frame = Frame(self,width=200)
    result_label = Label(result_frame,text="Result Frame")
    self.query_result_dbTreeView = dbTreeView(result_frame,{'A','B'},26)
    self.query_result_dbTreeView.column('#0', minwidth=100,anchor=CENTER, stretch=NO)
    self.query_result_dbTreeView.tag_configure('income', background='greenyellow')
    self.query_result_dbTreeView.tag_configure('expens', background='mistyrose')
    self.query_result_dbTreeView.tag_configure('hirarchy', background='white smoke')
    self.query_result_dbTreeView.tag_configure('hirarchy1', background='gainsboro')
    self.query_result_dbTreeView.tag_configure('hirarchy2', background='lightgray')
    self.query_result_dbTreeView.tag_configure('hirarchy3', background='lightgrey')
    self.query_result_dbTreeView.tag_configure('hirarchy4', background='silver')
    self.query_result_dbTreeView.tag_configure('hirarchy5', background='darkgray')
    self.query_result_dbTreeView.tag_configure('hirarchy6', background='darkgrey')
    self.query_result_dbTreeView.tag_configure('hirarchy7', background='gray')
    verscrlbar = ttk.Scrollbar(result_frame,
                           orient ="vertical",
                           command = self.query_result_dbTreeView.yview)
    self.query_result_dbTreeView.configure(xscrollcommand = verscrlbar.set)
    self.query_result_dbTreeView.bind("<Button-3>", self.OnRightClick)
    self.query_result_dbTreeView.bind("<Button-1>", self.OnSingleClick)    
    
    
  # result frame grid  elements  
    result_label.grid(column=0, row=0,sticky=tk.W)
    self.query_result_dbTreeView.grid(column=0, row=1,sticky='nsew')
    verscrlbar.grid(column=0, row=1,sticky='nse')

  # buttons data  frame create   elements 
    buttons_data_frame = Frame(self)
    buttons_data_label = Label(buttons_data_frame,text="Data Buttons")
    self.edit_data_button = Button(buttons_data_frame,text="Edit Data",command=self.open_edit_window,font=f_button)
    self.insert_data_button = Button(buttons_data_frame,text="Insert Data",command=self.open_insert_window,font=f_button)
    self.copy_data_button = Button(buttons_data_frame,text="Copy Data",command=self.open_copy_window,font=f_button)
    self.delete_data_button = Button(buttons_data_frame,text="Delete Data",command= self.delete_row,font=f_button)
    self.show_data_button = Button(buttons_data_frame,text="Show Data",command = self.open_show_window,font=f_button)
    self.plot_data_button = Button(buttons_data_frame,text="Plot Data",command=self.open_plot_window,font=f_button)
    self.detail_data_button = Button(buttons_data_frame,text="Detail Data",command=self.open_detail_window,font=f_button)
    self.sum_data_button = Button(buttons_data_frame,text="Sum Data",command=self.openSumWindow,font=f_button)

  # buttons data  frame grid the    elements 
    buttons_data_label.grid(column=0, row=0,pady=10,sticky=tk.W)
    self.edit_data_button.grid(column=0, row=1,pady=10,sticky=tk.W)
    self.insert_data_button.grid(column=0, row=2,pady=10,sticky=tk.W)
    self.copy_data_button.grid(column=0, row=3,pady=10,sticky=tk.W)
    self.delete_data_button.grid(column=0, row=4,pady=10,sticky=tk.W)
    self.show_data_button.grid(column=0, row=5,pady=10,sticky=tk.W)
    self.plot_data_button.grid(column=0, row=6,pady=10,sticky=tk.W)
    self.detail_data_button.grid(column=0, row=7,pady=10,sticky=tk.W)
    self.sum_data_button.grid(column=0, row=8,pady=10,sticky=tk.W)


  # all frames arrange grid 
    query_header_frame.grid(column=1, row=0,sticky=tk.W)
    query_frame.grid(column=1, row=1,sticky=tk.W)
    buttons_query_frame.grid(column=0, row=1,sticky=tk.W)
    result_frame.grid(column=1, row=2,sticky=tk.EW,columnspan = 3)
    buttons_data_frame.grid(column=0, row=2,sticky=tk.W)

    self.populate_query_data(query_id,None)

  def enable_disable_result_buttons ( self,button_state  ):
     # if the button state is DIABLED - then disable all buutons
     # if normal then enable all according to the options in the query text

    if button_state =="DISABLED" :    
        #color_column = get_json_value(temp_json,'color_column')
        #hirarchy_column = get_json_value(temp_json,'hirarchy_column')
        self.edit_data_button.config(state = DISABLED)
       # edit_line_button.config(state = NORMAL)

        self.insert_data_button.config(state = DISABLED)
      #  insert_line_button.config(state = NORMAL)

        self.copy_data_button.config(state = DISABLED)
      #  delete_line_button.config(state = NORMAL)

        self.show_data_button.config(state = DISABLED)
      #  show_line_button.config(state = NORMAL)

        self.plot_data_button.config(state = DISABLED)
      #  plot_line_button.config(state = NORMAL)

        self.detail_line_button.config(state = DISABLED)
      #  detail_line_button.config(state = NORMAL)

        self.sum_data_button.config(state = DISABLED)
      #  detail_line_button.config(state = NORMAL)

        self.back_query_button.config(state = DISABLED)
      #  back_line_button.config(state = NORMAL)

        self.open_query_args_button.config(state = DISABLED)
      #  back_line_button.config(state = NORMAL)
    else :
        #temp_json= get_json_param (self.query_scrolled_text.get('1.0',END),'{','}')
        #table_name = get_json_value(temp_json,'table_name')
        #pk_column1 = get_json_value(temp_json,'pk_column1')
        param_table_name_list = len(self.applQuery1.get_param_name_list_for_param('table_name'))
        param_pk_list  = len(self.applQuery1.get_param_name_list_for_param('primary_key'))
        param_plot_list = len(self.applQuery1.get_param_name_list_for_param('plot'))
        param_sum_list = len(self.applQuery1.get_param_name_list_for_param('sum'))
        param_back_list  = len(self.query_history_list)
        param_dialog_list = len(self.applQuery1.get_param_name_list_for_param('dialog'))
        param_input_list = len(self.applQuery1.get_param_name_list_for_param('input'))
        
        if param_table_name_list >0   and param_pk_list > 0  :
          self.edit_data_button.config(state = NORMAL) 
          self.insert_data_button.config(state = NORMAL)
          self.delete_data_button.config(state = NORMAL)
          self.copy_data_button.config(state = NORMAL)
          self.show_data_button.config(state = NORMAL)
        else :
          self.edit_data_button.config(state = DISABLED) 
          self.insert_data_button.config(state = DISABLED)
          self.delete_data_button.config(state = DISABLED)
          self.copy_data_button.config(state = DISABLED)
          self.show_data_button.config(state = DISABLED)


        if param_plot_list > 0 :
          self.plot_data_button.config(state = NORMAL)
        else:
          self.plot_data_button.config(state = DISABLED)

##        detail = get_json_value(temp_json,'detail_query1')
##        if detail is not None:
##          self.detail_line_button.config(state = NORMAL)
##        else :
##          self.detail_line_button.config(state = DISABLED)  

        #sum_column = get_json_value(temp_json,'sum_column')
        if param_sum_list > 0 :
          self.sum_data_button.config(state = NORMAL)
        else :
          self.sum_data_button.config(state = DISABLED)  


        if param_back_list >0 :
           self.back_query_button.config(state = NORMAL)
        else :
           self.back_query_button.config(state = DISABLED)

        if param_dialog_list >0 or param_input_list > 0 :
           self.open_query_args_button.config(state = NORMAL)
        else :
           self.open_query_args_button.config(state = DISABLED)         
         
   
  def clear_query_gui_items(self):


    self.query_id_entry.delete(0, END)
    self.query_name_entry.delete(0, END)
    self.query_type_combobox.delete(0, END)
    self.query_category_combobox.delete(0, END)
    self.query_usage_entry.delete(0, END)
    self.query_scrolled_text.delete('0.0', tk.END)
    self.query_result_dbTreeView.clear_all_items()
    #self.query_column_dbTreeView.clear_all_items()
    #self.query_param_dbTreeView.clear_all_items()

    
# this function runs only the result section
# it must run only after the populate_query_data
# in genarrl we want this function to run the query but if
# we need it go back from sort then we dont want to rerun and
# we do it in the data frame so we will use rerun query = 'n'
  def popuate_query_data_result (self,rerun_query='y'):
  
    self.query_result_dbTreeView.clear_all_items()
        # check if there is a need to rerun the query 
    if rerun_query =='y' :
       # we check to see if there args in query text but no ags in appl_query
       # in this cas we need to pop up input window to get args for the query
##       args_in_query_text = self.applQuery1.get_args_number_in_query_text()
##       if self.applQuery1.args is None :
##          args_in_appl_query_list = 0
##       else:
##          args_in_appl_query_list = len(self.applQuery1.args)
##
##     #  print(  self.applQuery1.get_query_type())
##       if args_in_query_text != args_in_appl_query_list and self.applQuery1.get_query_type() != 'procedure':
##           print(args_in_query_text,args_in_appl_query_list)
##           dialog_param_list = self.applQuery1.get_values_list_for_param('dialog')         
##           fkW = InputDataWindow(self,dialog_param_list)
##       else:
           self.applQuery1.run_query_data()
       
    # we get the color column list ( actualy it is  only one value in the list )        
    color_column_list = self.applQuery1.get_values_list_for_param('color_column')
    if len(color_column_list) == 0  :
       color_column = None
    else :
       color_column = color_column_list[0]
       
    hirarchy_column_list = self.applQuery1.get_values_list_for_param('hirarchy_column')
    if len(hirarchy_column_list) == 0  :
       hirarchy_column = None
    else :
       hirarchy_column = hirarchy_column_list[0]

   # we get the dictionary of column width
    mq1 = MyQuery(mydbcon,"select * from appl_query_column_width",{})
    mq1.execute_pandas_query()
    mq1_df_result = mq1.query_pandas_result_df
    column_dictionary = dict(mq1_df_result.values)
    
    self.query_result_dbTreeView.populate_from_data_frame(self.applQuery1.df_result,color_column,column_dictionary,hirarchy_column)
##    #check to see if there is   a param for color_column
##    if len(color_column_list) == 0  :
##       self.query_result_dbTreeView.populate_from_data_frame(self.applQuery1.df_result,None,column_dictionary)
##    else :
##       self.query_result_dbTreeView.populate_from_data_frame(self.applQuery1.df_result,color_column_list[0],column_dictionary)
    
# this function populate all the query item except from the result
# this is done due to the reason of detail query where we must find args before run

  def populate_query_data(self,query_id,args):
    self.applQuery1 = ApplQuery (mydbcon,query_id,args)

#    self.applQuery1.update_query_columns()
    
    self.clear_query_gui_items()
        
    # populate  header data 
    self.query_id_entry.insert(0,self.applQuery1.dic_properties["query_id"][0]  )
    self.query_name_entry.insert(0,self.applQuery1.dic_properties["query_name"][0]  )
    self.query_type_combobox.insert(0,self.applQuery1.dic_properties["query_type"][0]  )
    self.query_category_combobox.insert(0,self.applQuery1.dic_properties["query_category"][0]  )
    self.query_usage_entry.insert(0,self.applQuery1.dic_properties["query_usage"][0]  )
    self.query_scrolled_text.insert('1.0',self.applQuery1.dic_properties["query_text"][0]  )

    self.enable_disable_result_buttons('ENABLE')


  def find_detail_queries(self):
      self.applQuery1.find_detail_queries()
   #   print(self.applQuery1.detail_queries)
      
  def new_query(self):
    self.clear_query_gui_items()
   # self.applQuery1.new_appl_query(self)

    self.applQuery1 = ApplQuery.new_appl_query(self)
    #result_query,cursor_desc1 = execute_sql_no_except(mydbcon, "select max(query_id) from appl_query")
    #query_id = int(result_query[0][0])
    self.populate_query_data(self.applQuery1.get_query_id(),None)
    #self.popuate_query_data_result()

  def exp_backup (self ):
      backup_db_to_excel (mydbcon)

  def open_query(self,query_name):
    self.clear_query_gui_items()
   # self.applQuery1.new_appl_query(self)

    self.applQuery1 = ApplQuery.open_appl_query(self,query_name)
    #result_query,cursor_desc1 = execute_sql_no_except(mydbcon, "select max(query_id) from appl_query")
    #query_id = int(result_query[0][0])
    self.populate_query_data(self.applQuery1.get_query_id(),None)
    
  def execute_query(self):      
    self.applQuery1.set_query_text(self.query_scrolled_text.get('1.0','end'))
    #self.applQuery1.run_query_data()
    self.popuate_query_data_result()
    self.applQuery1.update_query_columns()   

  def open_load_window (self):
      loadW = LoadWindow (self)

  def open_query_window (self):
#      query_name =None
      fkW = OpenWindow(self)
#      print("in function")
#      print(self.query_name_return)
#      print("out function")

  def open_edit_window (self):

     # table_name = self.applQuery1.get_values_list_for_param('table_name')[0]
     # primary_keys_list = self.applQuery1.get_values_list_for_param('primary')
     # fk_list = self.applQuery1.get_values_list_for_param('fk')
     # fkW = EditTableWindow(self,'test',{'test1'})
      fkW = EditTableWindow(self,self.applQuery1,'y')

  def open_insert_window (self):

    #  table_name = self.applQuery1.get_values_list_for_param('table_name')[0]
     # primary_keys_list = self.applQuery1.get_values_list_for_param('primary')
    #  fk_list = self.applQuery1.get_values_list_for_param('fk')
     # fkW = EditTableWindow(self,'test',{'test1'})
     # fkW = InsertTableWindow(self,table_name,primary_keys_list,fk_list)
     fkW = InsertTableWindow(self,self.applQuery1)

  def open_copy_window (self):

     # table_name = self.applQuery1.get_values_list_for_param('table_name')[0]
     # primary_keys_list = self.applQuery1.get_values_list_for_param('primary')
     # fk_list = self.applQuery1.get_values_list_for_param('fk')
     # fkW = EditTableWindow(self,'test',{'test1'})
      fkW = CopyTableWindow(self,self.applQuery1)


  def open_show_window (self):

     # table_name = self.applQuery1.get_values_list_for_param('table_name')[0]
     # primary_keys_list = self.applQuery1.get_values_list_for_param('primary')
     # fk_list = self.applQuery1.get_values_list_for_param('fk')
     # fkW = EditTableWindow(self,'test',{'test1'})
     # fkW = ShowTableWindow(self,table_name,primary_keys_list,fk_list)
     fkW = ShowTableWindow(self,self.applQuery1)


  def open_args_window (self):

      dialog_param_list = self.applQuery1.get_values_list_for_param('dialog')
      input_param_list = self.applQuery1.get_values_list_for_param('input')
      args_list = self.applQuery1.get_query_args()

     # fkW = EditTableWindow(self,'test',{'test1'})
      fkW = EditArgsWindow(self.applQuery1,input_param_list,dialog_param_list,args_list)


#   this proceure delete the row in the treeview 
  def delete_row (self):

      # get the table_name and the primary lst 
      table_name = self.applQuery1.get_values_list_for_param('table_name')[0]
      primary_key_list = self.applQuery1.get_values_list_for_param('primary')

      #print(table_name)
      #print(primary_key_list)

      #  buid the delete  command
      
      delete_sql = 'delete from  '+ table_name
      args = list()
      # gte the values from the treeview 
      curItem = self.query_result_dbTreeView.focus()
      curItem_text = self.query_result_dbTreeView.item(curItem).get('values')
      delete_where_str = " where "

      #print(self.applQuery1.df_columns['column_name'].tolist())
      temp_counter = 0
      # loop over the columns of the tbale and match it with the primary key
      # and whe matched get the appropruate value from the treeview 
      for i , col in enumerate(self.applQuery1.df_columns['column_name'].tolist()):
        # print(col)
         if col in primary_key_list :   # if the col is aprimary key we add it to where claose
            if temp_counter == 0 :
               delete_where_str = delete_where_str + col + " = ? "
            else:
               delete_where_str = delete_where_str +" and " +col + " = ? "
               
            temp_counter = temp_counter +1 
            args.append(curItem_text[i])

      delete_sql = delete_sql + delete_where_str
     
      #print(delete_sql)
      #print(args)

      result_query,cursor_desc = execute_sql_no_except(mydbcon,delete_sql,args)
      result_query,cursor_desc = execute_sql_no_except(mydbcon,'commit')


  def show_query_params(self):
      #print("in show parameters")
      self.run_detail_query('det_appl_query_param',[self.applQuery1.get_query_id()])

  def show_query_columns(self):
      self.run_detail_query('det_appl_query_columns',{self.applQuery1.get_query_id()})





      
  def run_detail_query (self,query_name,default_args):
      # this runs the detail query after it was choosed 
      #print(query_name)
      new_query_id =self.applQuery1.get_query_id_stat(query_name)
      # add the queery to the histpry querires 
      self.query_history_list.append(self.applQuery1)
      # add the sort 
      #self.query_sort_list.append(self.applQuery1.last_tree_view_column_name_sort,self.applQuery1.last_tree_view_column_name_sort,last_tree_view_sort)
      # get the data from the tree view 
      curItem = self.query_result_dbTreeView.focus()
      # add the current line to history treeview line
      self.dbtreeview_row_history_list.append(curItem)
      #self.treeview_sort_history_list = None
      
      # this is a list of values from the treee view 
      tree_view_line = self.query_result_dbTreeView.item(curItem)['values']

      # this is the headers of the tree view 
      tree_view_columns = self.query_result_dbTreeView['columns']

      # now we can populate the new query data to the screen
      self.populate_query_data(new_query_id,None)
      # gets the param list from the new query 
      param_name_list = self.applQuery1.df_params['param_name']
      #gets param_valus list from the new query 
      param_value_list = self.applQuery1.df_params['param_value']

      # now we loop over the params to get only the input_params values

##      print( tree_view_columns)
##      print( tree_view_line)
##      print( param_name_list)
##      print( param_value_list)

      args = list()
      for i,p_name in enumerate(param_name_list) :
          if "input_param" in p_name and len(tree_view_line) > 0 :
              p_col_name = param_value_list[i]
              for j ,t_col_name in enumerate(tree_view_columns):
                  if t_col_name == p_col_name :
                     args.append(tree_view_line[j] )
             
##      print(args)
      if len(args) == 0 :
         self.applQuery1.args=default_args
      else :
         self.applQuery1.args=args
         
      self.popuate_query_data_result()
      self.applQuery1.update_query_columns()
      self.enable_disable_result_buttons('ENABLE')

      
  def save_query(self):
    self.applQuery1.set_query_name(self.query_name_entry.get())
    self.applQuery1.set_query_type(self.query_type_combobox.get())
    self.applQuery1.set_query_category(self.query_category_combobox.get())
    self.applQuery1.set_query_text(self.query_scrolled_text.get('1.0','end'))
    print('_______________________________________')
    print('______________SAVED ___________________')
    print('_______________________________________')

    

  def back_query(self):
     #  we get the last qpplquery that was relevant 
    self.applQuery1 = self.query_history_list.pop()
    # we get the sort values fo the appl query before it is initialised
    last_tree_view_column_name_sort = self.applQuery1.last_tree_view_column_name_sort
    last_tree_view_sort = self.applQuery1.last_tree_view_sort
    #print( self.applQuery1.get_query_name())  
    #self.open_query(self.applQuery1.get_query_name())
    self.populate_query_data(self.applQuery1.query_id,self.applQuery1.args)

       

    # we initialise the result df        
    self.popuate_query_data_result()

    # we check if there was a previos sort column befire we go back
    # if there was then we keep it and sort the df_result buy the same sort
    # that was before
    if last_tree_view_column_name_sort is not None :
       #sort_column_name = self.applQuery1.last_tree_view_column_name_sort
       if last_tree_view_sort == 'ascending' :
          self.applQuery1.df_result = self.applQuery1.df_result.sort_values(by=last_tree_view_column_name_sort,ascending=True)
       else :
          self.applQuery1.df_result = self.applQuery1.df_result.sort_values(by=last_tree_view_column_name_sort,ascending=False)

       self.popuate_query_data_result('n')

    # we go to line in tree view before we jumped to detail 
    last_tree_view_item = self.dbtreeview_row_history_list.pop()
    self.query_result_dbTreeView.focus(last_tree_view_item)
    self.query_result_dbTreeView.selection_set(last_tree_view_item)
    self.enable_disable_result_buttons('ENABLE')



  def open_choose_query_window (self):
      cqW = OpenCooseQuery(self)
      
  def open_detail_window(self) :
      self.find_detail_queries()
      menu_list_names = self.applQuery1.detail_queries_df['query_name'].tolist()
      query_usage_list = self.applQuery1.detail_queries_df['query_usage'].tolist()
      input_param_count_list = self.applQuery1.detail_queries_df['input_param_count'].tolist()
      m = Menu(self, tearoff = 0)
      separator_counter = 0 
      for idx ,det in enumerate(menu_list_names ):
         
         if idx > 0 :
            if input_param_count_list[idx]< input_param_count_list[idx -1] :
                m.add_separator()
                separator_counter = 0 

         m.add_command(label = det, command=lambda x=det: self.run_detail_query(x,None))
         separator_counter = separator_counter +1
         if separator_counter == 3 :
            separator_counter =0
            m.add_separator()
            
            
         #m.add_separator()
      m.tk_popup(300,300)   
      def do_popup(event):
            try:
              m.tk_popup(event.x_root, event.y_root)
            finally:
              m.grab_release()


  def open_plot_window (self):
      #self.find_detail_queries()
      # get the plot parameters ' if there are several plot option the list will contain more then one item 
      menu_list = self.applQuery1.get_param_name_list_for_param('plot')
      # check the lem of the plot params
      # if its one them there is only one param then we start immediatly the plot 
      if len(menu_list) > 1 :      
         m = Menu(self, tearoff = 0)
         for det in menu_list :
            m.add_command(label = det, command=lambda x=det: self.run_plot_window(x))
            
         m.tk_popup(300,300)

      else :
         self.run_plot_window(menu_list[0])
         
      def do_popup(event):
            try:
              m.tk_popup(event.x_root, event.y_root)
            finally:
              m.grab_release()     

  def run_plot_window (self,plot_name):

        df = self.applQuery1.df_result
        #print (df)
        #df = df.cumsum()
        # the the plot parameters from the json
        plot_params = self.applQuery1.get_values_list_for_param(plot_name)
        plot_param_list = plot_params[0].split(',') 
        # the first object in the list is the plot type
        # and after that are all the required params for this type
        # the param for xy_kind are - x , y kin tht can be
        # line , bar , hist ,hbar
##    ‘bar’ or ‘barh’ for bar plots
##    ‘hist’ for histogram
##    ‘box’ for boxplot
##    ‘kde’ or ‘density’ for density plots
##    ‘area’ for area plots
##    ‘scatter’ for scatter plots
##    ‘hexbin’ for hexagonal bin plots
##    ‘pie’ for pie plots
#     more info can be found here - https://pandas.pydata.org/docs/user_guide/visualization.html
        if plot_param_list[0] == 'x_y_kind' :
 
           df.plot(x= plot_param_list[1],y = plot_param_list[2], kind= plot_param_list[3])
           #df.plot(x= "name" , y = ["value","value2"], kind= "bar")


         # this is multi bar which doesnt do   pivot just use culumns
         # it takes the column_list
        elif plot_param_list[0] == 'multi_bars_columns' :
           # this is for multi bars on the same index
           label=list( df[plot_param_list[1]].values)
           label2  = list()
           for text in label :
              # this is done in  order to fix lef to right issue in plot 
              label2.append( bidialg.get_display(text) )              

           replace_dict = dict(zip(label, label2))
           df.replace(replace_dict, inplace=True)

           # create list of columns for the result data frame
           len_plot_param_list = len(plot_param_list)
           selected_col_list = list()
           # add to it the index column
           selected_col_list.append(plot_param_list[1])
           #df_result = df[plot_param_list[1]]
           for i in range(1,len_plot_param_list):
             selected_col_list.append(plot_param_list[i])
           
           print(selected_col_list)
           df_result = df[selected_col_list]
           # set the forst column as an index 
           df_result = df_result.set_index(plot_param_list[1])
           df_result.plot(kind='bar')      
         # this is multi bar which do pivot manipulation for a requested column
        elif plot_param_list[0] == 'multi_bars' :
           # this is for multi bars on the same index
           label=list( df[plot_param_list[2]].values)
           label2  = list()
           for text in label :
              # this is done in  order to fix lef to right issue in plot 
              label2.append( bidialg.get_display(text) )              

           replace_dict = dict(zip(label, label2))
           df.replace(replace_dict, inplace=True)
           
           df.pivot(index=plot_param_list[1], columns=plot_param_list[2], values=plot_param_list[3]).plot(kind='bar')           
        elif plot_param_list[0] == 'multi_bars_stacked' :
           # this is for multi bars on the same index and stacked togetehr on the same bar
           label=list( df[plot_param_list[2]].values)
           label2  = list()
           for text in label :
              # this is done in  order to fix lef to right issue in plot 
              label2.append( bidialg.get_display(text) )              

           replace_dict = dict(zip(label, label2))
           df.replace(replace_dict, inplace=True)
           #print(label)
           #print(label2)
           df.pivot(index=plot_param_list[1],columns=plot_param_list[2], values=plot_param_list[3]).plot(kind='bar',stacked=True)
        elif plot_param_list[0] == 'pie' :
           # this for a pie plot - the first column is the label and the second is values
           label=list( df[plot_param_list[1]].values)
           label2 = list ()
           for text in label :
              # this is done in  order to fix lef to right issue in plot 
              label2.append( bidialg.get_display(text) )
                      
           df.plot(kind='pie' ,labels =label2 ,  y=plot_param_list[2],).legend(loc='center left', bbox_to_anchor=(1, 0.5))           
           #df.plot(kind='pie'  ,  y=plot_param_list[2],) 

        elif plot_param_list[0] == 'area':
           label=list( df[plot_param_list[2]].values)
           label2  = list()
           for text in label :
              # this is done in  order to fix lef to right issue in plot 
              label2.append( bidialg.get_display(text) )              

           replace_dict = dict(zip(label, label2))
           df.replace(replace_dict, inplace=True)

           df = df.pivot(index=plot_param_list[1], columns=plot_param_list[2], values=plot_param_list[3]  )
           #index=plot_param_list[1], columns=plot_param_list[2], values=plot_param_list[3])
           df.plot.area()           
           #df.plot(kind='pie'  ,  y=plot_param_list[2],) 
        
        elif plot_param_list[0] == 'multi_line':
            fig,ax = plt.subplots()
          # df.pivot_table(values=plot_param_list[3],index=plot_param_list[1],columns=plot_param_list[2]).plot(kind = 'line')
            plot_list = df[plot_param_list[1]].unique().tolist()
            value_list = list()
            index_list= list()
            for index in plot_list :
               #print(index)
               label2=bidialg.get_display(index)
               df[df[plot_param_list[1]]== index ].plot(x= plot_param_list[2],y = plot_param_list[3],ax = ax, label = label2)
              # ax.plot(df[plot_param_list[1]== index].plot_param_list[2],df[plot_param_list[1]== index].plot_param_list[3],label=index)
            #   df2.plot(x= plot_param_list[2],y = plot_param_list[3],kind=line)
            
      # this calculate simple moving average 
        elif plot_param_list[0] == 'sma':
          # add ma3 column to the data frame


           # set the index for x axis 
          df.set_index(plot_param_list[1], inplace=True)
            # calc the len of the param each value aboe 3 will be the sma value to calc 
          plot_param_len = len(plot_param_list)

          plt.plot(df.index,df[plot_param_list[2]].values , label='data')
          for i in range(3,plot_param_len):
             sma_string  = 'sma'+plot_param_list[i]
             # this adds the sma columns to the data frame 
             df[sma_string] = df[plot_param_list[2]].rolling(window=int(plot_param_list[i])).mean()
##          df['sma6'] = df[plot_param_list[2]].rolling(window=6).mean()
##          df['sma9'] = df[plot_param_list[2]].rolling(window=9).mean()
             plt.plot(df.index,df[sma_string].values , label=sma_string)
##          plt.plot(df.index,df["ma3"].values , label='ma3')
##          plt.plot(df.index,df["ma6"].values , label='ma6')
##          plt.plot(df.index,df["ma9"].values , label='ma9')
          # show the legend
          plt.legend()
          # rotate x labels 90 degrees
          plt.xticks(rotation=90)

          
      # this calculate exponenital  moving average 

        elif plot_param_list[0] == 'ema':
          # add ma3 column to the data frame


           # set the index for x axis 
          df.set_index(plot_param_list[1], inplace=True)
            # calc the len of the param each value aboe 3 will be the sma value to calc 
          plot_param_len = len(plot_param_list)

          plt.plot(df.index,df[plot_param_list[2]].values , label='data')
          for i in range(3,plot_param_len):
             ema_string  = 'ema'+plot_param_list[i]
             # this adds the ems column to the data frame             
             df[ema_string] = df[plot_param_list[2]].ewm(span=int(plot_param_list[i]), adjust=False, min_periods=0).mean()
##          df['sma6'] = df[plot_param_list[2]].rolling(window=6).mean()
##          df['sma9'] = df[plot_param_list[2]].rolling(window=9).mean()
             plt.plot(df.index,df[ema_string].values , label=ema_string)
##          plt.plot(df.index,df["ma3"].values , label='ma3')
##          plt.plot(df.index,df["ma6"].values , label='ma6')
##          plt.plot(df.index,df["ma9"].values , label='ma9')
          # show the legend
          plt.legend()
          # rotate x labels 90 degrees
          plt.xticks(rotation=90)
          
        elif plot_param_list[0] == 'hist':
           # df.plot.hist(column=[plot_param_list[1]], by=plot_param_list[2])
             df.plot.hist(column=[plot_param_list[1]] )


        elif plot_param_list[0] == 'box':
          # we need params for index , columns and values
          # the data will be for exmple like this
              
          #  label    2017   2018   2019
          #  aa         20    30      40
          #  bb         23    40    34
          #
          #
          #
          #
             index1 = plot_param_list[1]
             columns1 = plot_param_list[2]
             values1  = plot_param_list[3]

             pivot_df = df.pivot_table(index=[index1],columns=columns1,values=values1)
             print(pivot_df)
             pivot_df.plot.box();
             plt.xticks(rotation=90)


        plt.ticklabel_format(style='plain', axis='y')
        mng = plt.get_current_fig_manager()
        #mng.full_screen_toggle()
        plt.show()

  def OnRightClick(self, event):
        #self.query_result_dbTreeView.focus_set()
     #   self.query_result_dbTreeView.focus(0)
     #   self.query_result_dbTreeView.selection_set(0)
     #   item = self.query_result_dbTreeView.selection()[0]
        #print("you clicked on", item)
        region = self.query_result_dbTreeView.identify("region", event.x, event.y)
        column = self.query_result_dbTreeView.identify("column", event.x, event.y)
        #print ("you clicked" ,region,column)
        if region =='heading' :
           #this is one counter less when you choose columnm in treeview 
          column_number = int(column.replace("#",""))
           #we get the column name by geting list of column and then convert to list and then get the appropriate value 
          column_name =self.applQuery1.df_columns['column_name'].to_list()[column_number-1]
          #print(self.applQuery1.get_unique_column_values(column_name) )
          #print(column_name)
          column_values = self.applQuery1.get_unique_column_values(column_name)
          #print(column_values)
          fkW = FilterWindow(self,column_name,column_values,1)
          
##          self.applQuery1.df_result = self.applQuery1.df_result.sort_values(by=column_name,ascending=False)
##          self.popuate_query_data_result('n')
##          self.applQuery1.last_tree_view_sort = 'descending'
##          self.applQuery1.last_tree_view_column_name_sort = column_name
##         # print(self.applQuery1.last_tree_view_sort,self.applQuery1.last_tree_view_column_name_sort)

  def OnSingleClick(self, event):
     #   self.query_result_dbTreeView.focus(0)
     #   self.query_result_dbTreeView.selection_set(0)
     #   item = self.query_result_dbTreeView.selection()[0]
        #print("you clicked on", item)
        region = self.query_result_dbTreeView.identify("region", event.x, event.y)
        column = self.query_result_dbTreeView.identify("column", event.x, event.y)
        #print ("you clicked" ,region,column)
        if region =='heading' :
          column_number = int(column.replace("#",""))
           #we get the column name by geting list of column and then convert to list and then get the appropriate value 
          column_name =self.applQuery1.df_columns['column_name'].to_list()[column_number-1]
          if self.applQuery1.last_tree_view_sort == 'descending' :
             self.applQuery1.df_result = self.applQuery1.df_result.sort_values(by=column_name,ascending=True)
             self.applQuery1.last_tree_view_sort = 'ascending'
          else:
             self.applQuery1.df_result = self.applQuery1.df_result.sort_values(by=column_name,ascending=False)
             self.applQuery1.last_tree_view_sort = 'descending'
          
          self.popuate_query_data_result('n')
##          self.applQuery1.last_tree_view_sort = 'ascending'
##          self.applQuery1.last_tree_view_sort = 'descending'
          self.applQuery1.last_tree_view_column_name_sort = column_name

  def openSumWindow (self):
    #print("sum")
    sum_column_list = self.applQuery1.get_values_list_for_param('sum')  
##    temp_json= get_json_param (self.query_scrolled_text.get('1.0',END),'{','}')
##    sum_column = int(get_json_value(temp_json,'sum_column') )
##
##    table_columns = list()
##    #  run the query
##    query_text = self.query_scrolled_text.get('1.0','end')
##    result_query,cursor_desc = execute_sql_no_except(mydbcon,query_text)
##    # populate the columns list from the cursor description
##    for idx ,head_text in enumerate(cursor_desc) :
##       table_columns.append(head_text[0])

    # get the  column name from wich we need to calc the sum
##    sum_col_name = table_columns[sum_column]
##    #create the query text as : select sum(..) from (  the original query ) 
##    query_text = "select sum("+sum_col_name+"),avg("+sum_col_name+"),count( "+ sum_col_name + " ) from ( "  \
##      + self.query_scrolled_text.get('1.0','end') + ")"
##    result_query,cursor_desc = execute_sql_no_except(mydbcon,query_text)
    temp_message = ''    
    for column_name in sum_column_list :
        count_val = self.applQuery1.df_result[column_name].count()
        sum_val=self.applQuery1.df_result[column_name].sum()
       # avg_val=self.applQuery1.df_result[column_name].average()
        min_val=self.applQuery1.df_result[column_name].min() 
        max_val=self.applQuery1.df_result[column_name].max()
        mean_val= self.applQuery1.df_result[column_name].mean()
       
        temp_message =temp_message + "result for column  " +column_name + "\n"   \
      + " __________________________________\n" \
      + " SUM IS : " + str(sum_val )+ "\n"    \
      + " COUNT IS : " + str(count_val ) + "\n" \
      + " Average IS :" + str(mean_val ) + "\n" \
      + " MIN IS : " + str(min_val ) + "\n" \
      + " MAX IS : " + str(max_val ) + "\n" \
      + " __________________________________\n"
      
    messagebox.showinfo(title="Sum data " , message=temp_message)



##--------------------------------   class OpenWindow------------------------
#  this class open combobox to choose new queries to run , edit  ...
##------------------------------------------------------------------------------
    
class OpenWindow(tk.Tk):
    # this open the open query window tp chhose for appl query 

    #mainWindowObj = None
    
    def __init__(self,mainWindowObj):
      #self.master = master
      self.mainWindowObj = mainWindowObj
      self.newWindow = tk.Toplevel(mainWindowObj.master)
      self.newWindow.title("Edit Window")
 
    # sets the geometry of toplevel
      self.newWindow.geometry("400x700")
    # set this window as modal 
      self.newWindow.grab_set()
      label = Label(self.newWindow,text="Please select a query:")
      label.pack(fill=tk.X, padx=5, pady=5)

      self.selected_query = tk.StringVar()
      self.query_cb = ttk.Combobox(self.newWindow, textvariable=self.selected_query)

      mq1 = MyQuery(mydbcon,"select query_name from appl_query",{})
      mq1.execute_pandas_query()
      df_result = mq1.query_pandas_result_df
      #print(df_result['query_name'])
      self.query_cb['values']=df_result['query_name'].tolist()
      #query_cb['values']=('aa','bb','cc')
      self.query_cb.pack(fill=tk.X, padx=5, pady=5)

      def query_changed(event):
    #""" handle the query changed event """
#        print("hi")
#        print ( self.selected_query.get() )
        self.mainWindowObj.query_name_return = self.selected_query.get()
#        print ( self.mainWindowObj.query_name_return )
        self.mainWindowObj.open_query(self.selected_query.get())
        self.newWindow.destroy()
       #print(self.query_cb.get())
        
      self.query_cb.bind('<<ComboboxSelected>>', query_changed)    

    #query_cb.bind('<<ComboboxSelected>>', query_changed)


class  EditArgsWindow(tk.Tk):
   #def __init__(self,mainWindowObj,input_param_list,dialog_param_list,args_list) :
   def __init__(self,applQuery1,input_param_list,dialog_param_list,args_list) :
      #self.dialog_param_list = dialog_param_list
      #self.args_list = args_list
      
      #self.mainWindowObj = mainWindowObj
      #self.newWindow = tk.Toplevel(mainWindowObj.master)
      self.newWindow = tk.Toplevel()
      self.applQuery1 = applQuery1
      self.newWindow.title("Edit ARGS ")      
     # sets the geometry of toplevel
      self.newWindow.geometry("400x500")
    # set this window as modal 
      self.newWindow.grab_set()

      # this will hold the label, and entry elements
      row = list()
      self.lab = list()
      self.ent = list()

   # we create empty window with column list but without vlaues
      #print(dialog_param_list)
      combined_list  = list()
      if input_param_list is not None :
        combined_list = combined_list + input_param_list

      if dialog_param_list is  not None :
         combined_list = combined_list + dialog_param_list
       
      for idx,dialog_param in enumerate(combined_list ) :
          row.append (Frame(self.newWindow) )
          self.lab.append( Label(row[idx], width=30, text=dialog_param, anchor='w') )
          self.ent.append( Entry(row[idx ],width=15))
          # check if there is args values in the args list
          if args_list is not None :
            if len(args_list)> idx :
               self.ent[idx].insert(0,args_list[idx])

          row[idx].pack(side=TOP, fill=X, padx=5, pady=5)
          self.lab[idx].pack(side=LEFT)
#          self.ent[idx].pack(side=LEFT,expand=YES, fill=X)
          self.ent[idx].pack(side=LEFT)

      save_button =Button(self.newWindow,text="Save ",command=self.saveDataRecord)
      save_button.pack(side=TOP, padx=5, pady=5)


   def saveDataRecord(self):
      new_arg_list = list()
      # go thro the entry list and get from each entry the value and append it to the new arg list 
      for idx,e1 in enumerate(self.ent):
         new_arg_list.append(e1.get())

      # after that we set the new arg list of the appl query
      self.applQuery1.args = new_arg_list

           

      self.newWindow.destroy()        
 
##--------------------------------   class EditTableData------------------------
#  this class will help edit the columns and params of qppl_query data
##------------------------------------------------------------------------------

class EditTableWindow(tk.Tk):
  # def __init__(self,mainWindowObj,table_name,primary_key_list,populate_y_n,fk_list = None):
   def __init__(self,mainWindowObj,applQuery1,populate_y_n):
      self.applQuery1 = applQuery1 
      self.table_name = self.applQuery1.get_values_list_for_param('table_name')[0]
      fk_list = self.applQuery1.get_values_list_for_param('fk')
      self.primary_key_list = self.applQuery1.get_values_list_for_param('primary')
      self.auto_increment_list = self.applQuery1.get_values_list_for_param('auto_increment')


      self.mainWindowObj = mainWindowObj
      self.newWindow = tk.Toplevel(mainWindowObj.master)
      self.newWindow.title("Edit Data")
 
    # sets the geometry of toplevel
      self.newWindow.geometry("600x700")
    # set this window as modal 
      self.newWindow.grab_set()

   # get the vlaues from the paren window treeview

      curItem = mainWindowObj.query_result_dbTreeView.focus()
      curItem_text = mainWindowObj.query_result_dbTreeView.item(curItem).get('values')
      if populate_y_n == 'y' :
         args=list()
         string_where = " where "
         for i,pk in enumerate( self.primary_key_list):
            if i == 0 :
               string_where = string_where+ pk + " = ? "
            else:
               string_where = string_where +" and " +pk + " = ? " 
            args.append(mainWindowObj.query_result_dbTreeView.item(curItem).get('values')[i])
      else :
         string_where = ' where 1=?'
         args=[2]
                    
      mq1 = MyQuery( mydbcon,"select * from "+ self.table_name +string_where,args)
      mq1.execute_pandas_query()
      column_list = mq1.get_result_coulmns_list()

      # this will hold the label, and entry elements
      row = list()
      self.lab = list()
      self.ent = list()
      fk_button = list()
      fk_counter = 0
      auto_increment_field_list =  applQuery1.get_values_list_for_param("auto")
   # we create empty window with column list but without vlaues       
      for idx,field in enumerate(column_list ) :
         
          row.append (Frame(self.newWindow) )
          self.lab.append( Label(row[idx], width=15, text=field, anchor='w') )
          # we check if this field is auto incrmetent - if it is then we disabled it for edit 
          if field in auto_increment_field_list :
             self.ent.append( Entry(row[idx ],width=40,state='readonly'))
             
          else :
             self.ent.append( Entry(row[idx ],width=40))
          # create button for fk onlyy it there is an fk_list and the column matches the list 
          if len(fk_list) >0  and  field in  fk_list :
             fk_button.append(Button(row[idx ], text="open"+str(idx),width=10, command= lambda x= field , y = idx:  self.open_fk_window(x, y ) ))
                                     
          row[idx].pack(side=TOP, fill=X, padx=5, pady=5)
          self.lab[idx].pack(side=LEFT)
#          self.ent[idx].pack(side=LEFT,expand=YES, fill=X)
          self.ent[idx].pack(side=LEFT)

          if len(fk_list) >0 and  field in  fk_list :
             fk_button[fk_counter].pack(side=LEFT, expand=YES, fill=X)
             fk_counter = fk_counter +1 
             

      save_button =Button(self.newWindow,text="Save ",command=self.saveDataRecord)
      save_button.pack(side=TOP, padx=5, pady=5)

   # here we populate the values
   # only it poplutae y n is set to y = this is done so we can use the insert window also with this class
      if populate_y_n == 'y' :
         for i,col in enumerate(column_list) :
            if mq1.query_pandas_result_df[col][0] is not None :
              # if this was an auto incremnt field we enable it , enter the value and disable it again 
              if col in auto_increment_field_list :
                 self.ent[i].config(state='normal')
                 self.ent[i].insert(0,mq1.query_pandas_result_df[col][0])
                 self.ent[i].config(state='readonly')
              else :
                 self.ent[i].insert(0,mq1.query_pandas_result_df[col][0])
    
   def saveDataRecord(self):
      #  buid the update command 
     update_query = 'update '+ self.table_name + " set "
     args = list()
     update_field_str = "("
     update_values_str = "("
     field_cnt = 0
     for idx,e1 in enumerate(self.ent):
        # we only include non pk fields in the update clause 
         if self.lab[idx]["text"] not in self.primary_key_list :
            # if e1 is empty then we dont include it in the update statement 
             if e1.get() != '' :
                args.append(e1.get())
                if field_cnt != 0 :
                   update_field_str = update_field_str + ","+self.lab[idx]["text"]
                   update_values_str= update_values_str+",?"
                else :
                   update_field_str = update_field_str + self.lab[idx]["text"]
                   update_values_str= update_values_str + "?"

                field_cnt = field_cnt + 1
              
     update_field_str = update_field_str + ")"
     update_values_str = update_values_str +")"

     update_query = update_query + update_field_str +" = "+update_values_str
     update_where_str = " where "
     temp_counter = 0 
     for i,lab in enumerate( self.lab):
         col = self.lab[i]["text"]
         if col in self.primary_key_list :   # if the col is aprimary key we add it to where claose
            if temp_counter == 0 :
               update_where_str = update_where_str+ col + " = ? "
            else:
               update_where_str = update_where_str +" and " +col + " = ? "

            temp_counter = temp_counter +1
            args.append(self.ent[i].get())

     update_query = update_query + update_where_str

##     print(update_query)
##     print(args)
     result_query,cursor_desc = execute_sql_no_except(mydbcon,update_query,args)
     result_query,cursor_desc = execute_sql_no_except(mydbcon,'commit')

     self.newWindow.destroy()


   def open_fk_window (self,column_name, entry_idx):
      fkW = FkWindow(self,column_name,entry_idx)

class ShowTableWindow(EditTableWindow):
##   def __init__(self,mainWindowObj,table_name,primary_key_list,fk_list):
##       super().__init__( mainWindowObj,table_name,primary_key_list,'y',fk_list)
   def __init__(self,mainWindowObj,applQuery1):
         super().__init__( mainWindowObj,applQuery1,'y')


   def saveDataRecord(self):
       self.newWindow.destroy()


class InsertTableWindow(EditTableWindow):
   #def __init__(self,mainWindowObj,table_name,primary_key_list,fk_list):
   #    super().__init__( mainWindowObj,table_name,primary_key_list,'n',fk_list)
   def __init__(self,mainWindowObj,applQuery1):
        super().__init__( mainWindowObj,applQuery1,'n')
        
   def saveDataRecord(self):
      #  buid the update command 
     insert_sql = 'insert into  '+ self.table_name + "  "
     args = list()
     insert_field_str = "("
     insert_values_str = "("
     field_cnt = 0
     for idx,e1 in enumerate(self.ent):
                # we checj if there is a column with auto increnst
                # in this case we change the insert command and not use this column
                # for example inser into todo(todo_desc,todo_remark,..) values ('ssdsd','adad',...)
                # ths field todo_id is ommited to avoid sql error datatype mismatch
                # in this case there is no auto_incremnt param (len is 0 ) so we add the first value
             if field_cnt == 0  and len(self.auto_increment_list) > 0 :
                args.append(None)
             else:
                if e1.get() != '' :
                   args.append(e1.get())
                else :
                   args.append(None)

                
             if field_cnt != 0 :
                insert_field_str = insert_field_str + ","+self.lab[idx]["text"]
                insert_values_str= insert_values_str+",?"
             else : 
                insert_field_str = insert_field_str + self.lab[idx]["text"]
                insert_values_str= insert_values_str + "?"

             field_cnt = field_cnt + 1
              
     insert_field_str = insert_field_str + ")"
     insert_values_str = insert_values_str +")"

     insert_sql = insert_sql + insert_field_str +" values "+insert_values_str
##     update_where_str = " where "
##     temp_counter = 0 
##     for i,lab in enumerate( self.lab):
##         col = self.lab[i]["text"]
##         if col in self.primary_key_list :   # if the col is aprimary key we add it to where claose
##            if temp_counter == 0 :
##               update_where_str = update_where_str+ col + " = ? "
##            else:
##               update_where_str = update_where_str +" and " +col + " = ? "
##
##            temp_counter = temp_counter +1 
##            args.append(self.ent[i].get())
##
##     update_query = update_query + update_where_str
    # print(insert_sql)
    # print(args)

     result_query,cursor_desc = execute_sql_no_except(mydbcon,insert_sql,args)
     result_query,cursor_desc = execute_sql_no_except(mydbcon,'commit')

     self.newWindow.destroy()

class CopyTableWindow(EditTableWindow):
   def __init__(self,mainWindowObj,applQuery1):
       super().__init__( mainWindowObj,applQuery1,'y')

   def saveDataRecord(self):
     insert_sql = 'insert into  '+ self.table_name + "  "
     args = list()
     insert_field_str = "("
     insert_values_str = "("
     field_cnt = 0
     # iterate throw the entry in the window - this have the vlaues 
     for idx,e1 in enumerate(self.ent):
        
                # we checj if there is a column with auto increnst
                # in this case we change the insert command and not use this column
                # for example inser into todo(todo_desc,todo_remark,..) values ('ssdsd','adad',...)
                # ths field todo_id is ommited to avoid sql error datatype mismatch
                # in this case there is no auto_incremnt param (len is 0 ) so we add the first value
             if field_cnt == 0  and len(self.auto_increment_list) > 0 :
                args.append(None)
             else:
                if e1.get() != '' :
                   args.append(e1.get())
                else :
                   args.append(None)
                
             if field_cnt != 0 :
                insert_field_str = insert_field_str + ","+self.lab[idx]["text"]
                insert_values_str= insert_values_str+",?"
             else :
                insert_field_str = insert_field_str + self.lab[idx]["text"]
                insert_values_str= insert_values_str + "?"

             field_cnt = field_cnt + 1
              
     insert_field_str = insert_field_str + ")"
     insert_values_str = insert_values_str +")"

     insert_sql = insert_sql + insert_field_str +" values "+insert_values_str

     #print(insert_sql)
     #print(args)
     
     result_query,cursor_desc = execute_sql_no_except(mydbcon,insert_sql,args)
     result_query,cursor_desc = execute_sql_no_except(mydbcon,'commit')

     self.newWindow.destroy()




class OpenCooseQuery (tk.Tk):
  def __init__(self,parentWindowObj):

      # define stylefor the treeview data and header 
      style = ttk.Style()
      style.configure("Treeview", font=('Helvetica', 11))  # Set the font for the body
      style.configure("Treeview.Heading", font=('Helvetica', 12, 'bold'))

      global f_labels
      f_labels= font.Font(family=None, size=12, weight="bold")
      global f_button
      f_button= font.Font(family=None, size=12)
      global f_data
      f_data = font.Font(family=None, size=12, weight="normal")
      
      #self.master = master
      self.parentWindowObj = parentWindowObj
     # newWindow = tk.Toplevel(parentWindowObj.master)
      self.newWindow = tk.Toplevel()
      self.newWindow.title("Choose  Window")
   # sets the geometry of toplevel
      self.newWindow.geometry("700x800")
    # set this window as modal 
      self.newWindow.grab_set()

      # this will hold the data of the tree view in order to be able to sort it if needed
      self.df_result = None
      # this will hold the treeview column width in order to be able to sort it if needed
      self.column_dictionary = None
      
      # this is aframe for the object where we can choose the query 
      choose_query_frame = Frame(self.newWindow,width=25)
      
      #query_name_label = Label(choose_query_frame,text="Query Name",width=20)
      button_separator_1 = ttk.Separator(choose_query_frame,orient='horizontal')
      query_type_label = Label(choose_query_frame,text="Query Type",width=12,font=f_labels)
      query_category_label = Label(choose_query_frame,text="Query category",width=12,font=f_labels)

      #query_name_entry = Entry(choose_query_frame)
      button_separator_2 = ttk.Separator(choose_query_frame,orient='horizontal')
      self.query_type_combobox = ttk.Combobox(choose_query_frame, textvariable=None)
      self.query_category_combobox = ttk.Combobox(choose_query_frame, textvariable=None)

      #query_name_label.grid(column=0, row=0,sticky=tk.W)
      button_separator_1.grid(column=0, row=0,sticky=tk.W,padx=90)
      query_type_label.grid(column=1, row=0,sticky=tk.W)
      query_category_label.grid(column=2, row=0,sticky=tk.W)

      # fill the combobox values from query type 
      mq1 = MyQuery(mydbcon,"select query_type from appl_query_type",{})
      mq1.execute_pandas_query()
      df_result = mq1.query_pandas_result_df
      result_list = df_result['query_type'].tolist()
      # we add this for the option to select all from the query type 
      result_list.append('%')
      self.query_type_combobox['values']=result_list
      self.query_type_combobox.current(1)
      
      # fill the combobox values from query category 
      mq1 = MyQuery(mydbcon,"select category_name from appl_query_categories",{})
      mq1.execute_pandas_query()
      df_result = mq1.query_pandas_result_df
      result_list = df_result['category_name'].tolist()
      result_list.append('%')
      self.query_category_combobox['values']=result_list
      # choose the number ten  object wich is transactions
      self.query_category_combobox.current(12)

      #query_name_entry.grid(column=0, row=1,sticky=tk.W)
      button_separator_2.grid(column=0, row=1,sticky=tk.W,padx=90)
      self.query_type_combobox.grid(column=1, row=1,sticky=tk.W)
      self.query_category_combobox.grid(column=2, row=1,sticky=tk.W)


      # fill the initail rows in the tree view by query appl_query with args from type and actegory
      self.query_result_dbTreeView = dbTreeView(self.newWindow,{'Query Name','Query Type','Query category'},30)



      
      
      choose_query_frame.grid(column=0, row=0,sticky=tk.W)
      self.query_result_dbTreeView.grid(column=0, row=1,sticky=tk.W)
      # we run the procedure which poulate the tree fiew 
      self.populate_dbtreeview()

      def query_changed(event):
         #print('hi')
        #""" handle the query changed event """
         self.populate_dbtreeview()
       # self.mainWindowObj.query_name_return = self.selected_query.get()

       # self.mainWindowObj.open_query(self.selected_query.get())
       # self.newWindow.destroy()


      def treeview_choose(event):
          curItem = self.query_result_dbTreeView.focus()
          curItem_text = self.query_result_dbTreeView.item(curItem).get('values')
          #print(curItem_text)
          query_name = curItem_text[1]
          self.parentWindowObj.query_name_return = query_name
#        print ( self.mainWindowObj.query_name_return )
         # open the query in the parent window
          self.parentWindowObj.open_query(query_name)
         # self.newWindow.destroy()
          # if the query is  aprimary query then run it immediatly - else - it just populated
          if self.parentWindowObj.applQuery1.get_query_type_stat(query_name) == 'primary' :

             # we check to see if there args in query text but no ags in appl_query
             # in this cas we need to pop up input window to get args for the query
             args_in_query_text = self.parentWindowObj.applQuery1.get_args_number_in_query_text()
             if self.parentWindowObj.applQuery1.args is None :
                args_in_appl_query_list = 0
             else:
                args_in_appl_query_list = len(self.parentWindowObj.applQuery1.args)

           #  print(  self.applQuery1.get_query_type())
             if args_in_query_text != args_in_appl_query_list and self.parentWindowObj.applQuery1.get_query_type() != 'procedure':
               #  print(args_in_query_text,args_in_appl_query_list)
                 dialog_param_list = self.parentWindowObj.applQuery1.get_values_list_for_param('dialog')         
                 fkW = InputDataWindow(self.parentWindowObj.applQuery1,dialog_param_list)
             else :
                 self.parentWindowObj.popuate_query_data_result()
             
          self.newWindow.destroy()

      # this will hold the data frame of the results of the dbtreeview in order to be able to sort it in the futire 
      
      self.query_type_combobox.bind('<<ComboboxSelected>>', query_changed)
      self.query_category_combobox.bind('<<ComboboxSelected>>', query_changed) 

     # self.query_result_dbTreeView.bind("<Button-3>", treeview_choose)
      #self.query_result_dbTreeView.bind("<Double-Button>", treeview_choose)
      self.query_result_dbTreeView.bind("<Double-1>", treeview_choose)
      self.query_result_dbTreeView.bind("<Button-3>", self.OnRightClick)
      self.query_result_dbTreeView.bind("<Button-1>", self.OnSingleClick)

  def sort_dbtreeview(self):
      self.query_result_dbTreeView.clear_all_items()
      self.query_result_dbTreeView.populate_from_data_frame( self.df_result,None,self.column_dictionary)
     
      # this procedure  popultae the dbtreeview
  def populate_dbtreeview(self):
             # we clear all the items from the treeview 
            self.query_result_dbTreeView.clear_all_items()
            
            # we get the colomn width dictionary 
            mq1 = MyQuery(mydbcon,"select * from appl_query_column_width",{})
            mq1.execute_pandas_query()
            mq1_df_result = mq1.query_pandas_result_df
            self.column_dictionary = dict(mq1_df_result.values)
            #we get the data frame result  with args from the combo boxex 
            arg_list = list()
            arg_list.append(self.query_type_combobox.get())
            arg_list.append(self.query_category_combobox.get())
            mq2 = MyQuery(mydbcon,"select query_id,query_name,query_type,query_category,query_usage  \
                                    from appl_query \
                                    where query_type like  ? and query_category like ?   \
                                    order by query_usage desc",arg_list)
            mq2.execute_pandas_query()      
            self.df_result = mq2.query_pandas_result_df
            # we poulate the dbtreeview 
            self.query_result_dbTreeView.populate_from_data_frame( self.df_result,None,self.column_dictionary)


  def OnRightClick(self, event):
        #self.query_result_dbTreeView.focus_set()
        self.query_result_dbTreeView.focus(0)
        self.query_result_dbTreeView.selection_set(0)
        item = self.query_result_dbTreeView.selection()[0]
        #print("you clicked on", item)
        region = self.query_result_dbTreeView.identify("region", event.x, event.y)
        column = self.query_result_dbTreeView.identify("column", event.x, event.y)
        #print ("you clicked" ,region,column)
        if region =='heading' :
           #this is one counter less when you choose columnm in treeview 
          column_number = int(column.replace("#",""))
           #we get the column name by geting list of column and then convert to list and then get the appropriate value 
          column_name = self.query_result_dbTreeView["columns"][column_number-1]
          #print(column_name)
          self.df_result = self.df_result.sort_values(by=column_name,ascending=False)
          self.sort_dbtreeview()
          
      #    self.applQuery1.last_tree_view_sort = 'descending'

  def OnSingleClick(self, event):
        #self.query_result_dbTreeView.focus_set()
        self.query_result_dbTreeView.focus(0)
        self.query_result_dbTreeView.selection_set(0)
        item = self.query_result_dbTreeView.selection()[0]
        #print("you clicked on", item)
        region = self.query_result_dbTreeView.identify("region", event.x, event.y)
        column = self.query_result_dbTreeView.identify("column", event.x, event.y)
        #print ("you clicked" ,region,column)
        if region =='heading' :
          column_number = int(column.replace("#",""))
           #we get the column name by geting list of column and then convert to list and then get the appropriate value 
          #column_name =self.applQuery1.df_columns['column_name'].to_list()[column_number-1]
          column_name = self.query_result_dbTreeView["columns"][column_number-1]
          #print(column_name)
          self.df_result = self.df_result.sort_values(by=column_name,ascending=True)
          self.sort_dbtreeview()
          #self.applQuery1.last_tree_view_sort = 'ascending'

 # this classs get the column name and all of its dictinct values and open
 # a treeview to choose from this values
 # this values will be return to the main windows and enable filter th rows accordingly 
class FilterWindow (tk.Tk) :
   def __init__(self,parentWindowObj,column_name,column_values,entry_idx):
      #self.master = master
      self.parentWindowObj = parentWindowObj
     # newWindow = tk.Toplevel(parentWindowObj.master)
      self.newWindow = tk.Toplevel()
      self.newWindow.title("Filter Window")
      

    # Toplevel object which will
    # be treated as a new window
     # newWindow = Toplevel(root)

      self.filter_column_name = column_name
      self.filter_column_values = column_values
 
    # sets the title of the
    # Toplevel widget
     # newWindow.title("New Window")
 
    # sets the geometry of toplevel
      self.newWindow.geometry("400x700")
    # set this window as modal 
      self.newWindow.grab_set()
      

     # sets the geometry of toplevel
     # newWindow.geometry("400x700")
     # set this window as modal 
     # newWindow.grab_set()


      fk_label = Label(self.newWindow,text="Column label")
      self.fk_entry_search = Entry(self.newWindow)

      self.filter_TreeView = ttk.Treeview(self.newWindow,columns=column_name,show='headings')
      self.save_fk_button =Button(self.newWindow,text="SAVE ME "  ,command= lambda : self.save_fk())      

      self.fk_entry_search.pack(side=TOP)
      self.filter_TreeView.pack(side=TOP)

      self.save_fk_button.pack(side=TOP, padx=5, pady=5)


      self.populate_treeview(column_name,column_values)

      self.newWindow.bind('<Key>', lambda x=None : self.search2(self))

   def search2 (self,event):
   # we get the value from the searc gentry 
      substr_item = self.fk_entry_search.get()
      #print(self.filter_column_values)
      sub_filtered_column_list = [item for item in self.filter_column_values if item is not None and substr_item in str(item)]

      self.populate_treeview(self.filter_column_name,sub_filtered_column_list)

   def save_fk(self):
       curItem = self.filter_TreeView.focus()
       the_filter_value = self.filter_TreeView.item(curItem).get('values')[0]
      # print("hello ~ !",the_filter_value)
      # we set the dataframe result of the upper window to be  with filter
       upper_df_result = self.parentWindowObj.applQuery1.df_result
      # filtered_df = df[df['Name'] == 'Alice']
       try:
          the_filter_value_numeric = float(the_filter_value)
          filtered_result = upper_df_result[upper_df_result[self.filter_column_name] == the_filter_value_numeric]
       except ValueError:
          print(f"Error: Could not convert '{the_filter_value}' to a number.")
          filtered_result = upper_df_result[upper_df_result[self.filter_column_name] == the_filter_value]
       
                                         
       self.parentWindowObj.applQuery1.df_result = filtered_result 
      # after that we run populate data again 
       self.parentWindowObj.popuate_query_data_result('n')
       #ent[self.idx].value(the_fk_value0
    #   self.parentWindowObj.ent[self.idx].delete(0,END)
    #   self.parentWindowObj.ent[self.idx].insert(0,the_fk_value)
       self.newWindow.destroy()

   def populate_treeview(self,column_name,column_values) :
      
     # print(column_values)
    # clear all the items in the treview 
      for item in self.filter_TreeView.get_children():
         self.filter_TreeView.delete(item)

    #  tree_columns = list()
    #  tree_columns.append(column_name)              
      
          
      self.filter_TreeView.column(column_name, minwidth=0,anchor=CENTER, stretch=NO, width=200)
      self.filter_TreeView.heading(column_name, text=column_name,anchor=CENTER)
     
  # iterate throw the data  cursor and extratc data from tuple
      #print(result_query)
      for idx,col_value in enumerate(column_values) :
        tuple_value =  (col_value,)
       # print(col_value)
       # print(tuple_value)
        self.filter_TreeView.insert('',tk.END, values=(tuple_value),iid = idx )
       # self.filter_TreeView.insert('', tk.END, values=col_value  )
 #       if int(tup_list[2]) > 0 :
#             self.fk_dbTreeView.move(idx, idx-1,idx)





class FkWindow(tk.Tk) :

# we transfet this class the column_name and the entry idx - to return thr value to this entry
  def __init__(self,parentWindowObj,column_name,entry_idx):
      #self.master = master
      self.parentWindowObj = parentWindowObj
     # newWindow = tk.Toplevel(parentWindowObj.master)
      self.newWindow = tk.Toplevel()
      self.newWindow.title("Fk Window")
      

    # Toplevel object which will
    # be treated as a new window
     # newWindow = Toplevel(root)
 
    # sets the title of the
    # Toplevel widget
     # newWindow.title("New Window")
 
    # sets the geometry of toplevel
      self.newWindow.geometry("400x700")
    # set this window as modal 
      self.newWindow.grab_set()
      
    #  def open_fk (field_name,idx):
      
          #app = App()
      self.field_name = column_name
      self.idx = entry_idx
     
     # Toplevel object which will
     # be treated as a new window
     # newWindow = Toplevel(root)
 
     # sets the title of the
     # Toplevel widget
     # newWindow.title(field_name+" "+str(idx))
 
     # sets the geometry of toplevel
     # newWindow.geometry("400x700")
     # set this window as modal 
     # newWindow.grab_set()


      fk_label = Label(self.newWindow,text="FK label")
      self.fk_entry_search = Entry(self.newWindow)

      if self.field_name == 'account_type'  :
          columns = ['account_type','account_type_desc']
          self.query_text = 'select account_type,account_type_desc from accounts_type'
          self.where_clause ="where account_type like '%<text>%' "
      elif self.field_name == 'ref_account_id' or self.field_name == 'account_id'  :
          columns = ['account_id','account_desc']
          self.query_text = 'select account_id,account_name  from accounts'
          self.where_clause ="where account_name like '%<text>%' "
      elif self.field_name == 'label' or self.field_name == 'Parent_label':
          columns = ['label','parent_label','level']
          self.query_text = 'select label,parent_label,level  from labels'
          self.where_clause ="where label like '%<text>%' "
      elif self.field_name == 'src_account_id':
          columns = ['account_id','account_desc']
          self.query_text = 'select account_id,account_name  from accounts'
          self.where_clause ="where account_desc like '%<text>%' "

         
      columns = ['AA','BB']
          
      self.fk_dbTreeView = ttk.Treeview(self.newWindow,columns=columns,show='headings')
      self.save_fk_button =Button(self.newWindow,text="SAVE ME "  ,command= lambda : self.save_fk())

      self.populate_treeview(self.query_text)
      #self.populate_treeview_hirarchy(self.query_text)
##      result_query,cursor_desc = execute_sql_no_except(mydbcon,query_text)
##
##      
##      for header in columns:
##         # self.fk_dbTreeView.column(header, minwidth=0,anchor=CENTER, stretch=NO, width=self.width_length.get())
##          self.fk_dbTreeView.column(header, minwidth=0,anchor=CENTER, stretch=NO, width=150)
##          self.fk_dbTreeView.heading(header, text=header,anchor=CENTER)
##
##       # iterate throw the data  cursor and extratc data from tuple
##      for idx,line_tup in enumerate(result_query) :
##        #position_text = str(idx)+'.0'
##        # convert tuple to string 
##        tup_str = ''
##        tup_list = list()
##        for item in line_tup:
##          tup_str = tup_str + str(item).ljust(20)
##          tup_list.append(str(item))
##          
##        self.fk_dbTreeView.insert('', tk.END, values=tup_list)

      #curItem = fk_dbTreeView.focus()   
      #treeview_key = fk_dbTreeView.item(curItem).get('values')[0]
      
      fk_label.pack(side=TOP)
      self.fk_entry_search.pack(side=TOP)
      self.fk_dbTreeView.pack(side=TOP)

      self.save_fk_button.pack(side=TOP, padx=5, pady=5)

      self.newWindow.bind('<Key>', lambda x=None : self.search2(self))

  def save_fk(self):
       curItem = self.fk_dbTreeView.focus()
       the_fk_value = self.fk_dbTreeView.item(curItem).get('values')[0]
       #print("hello ~ !",the_fk_value)
       #ent[self.idx].value(the_fk_value0
       self.parentWindowObj.ent[self.idx].delete(0,END)
       self.parentWindowObj.ent[self.idx].insert(0,the_fk_value)
       self.newWindow.destroy()
       
  def search(self,event, item=''):
    #print ('hi i am in search')
    children = self.fk_dbTreeView.get_children(item)
    for child in children:
        #text = self.fk_dbTreeView.item(child, 'text')
        text = self.fk_dbTreeView.item(child).get('values')[0]
        #print(text)
       # if text.startswith(self.fk_entry_search.get()):
       # if self.fk_entry_search.get() in text :
            
        if text.find(self.fk_entry_search.get()) != -1:
            self.fk_dbTreeView.selection_set(child)
            #print("Found!")

  def search2(self,event, item=''):
   # print ('hi i am in search2')
    #new_where_clause = self.where_clause
   # new_where_clause = new_where_clause.replace("<text>",self.fk_entry_search.get())
    new_query_text = self.query_text +" "+ self.where_clause.replace("<text>",self.fk_entry_search.get())
   # print (new_query_text)
    self.populate_treeview(new_query_text)

  def populate_treeview(self,query_text) :
      
      result_query,cursor_desc = execute_sql_no_except(mydbcon,query_text)

    # clear all the items in the treview 
      for item in self.fk_dbTreeView.get_children():
         self.fk_dbTreeView.delete(item)

      tree_columns = list()
      for idx ,head_text in enumerate(cursor_desc) :

          tree_columns.append(head_text[0])              
          self.fk_dbTreeView['columns'] = tree_columns
          
      for header in tree_columns:
             self.fk_dbTreeView.column(header, minwidth=0,anchor=CENTER, stretch=NO, width=200)
             self.fk_dbTreeView.heading(header, text=header,anchor=CENTER)

  # iterate throw the data  cursor and extratc data from tuple
      #print(result_query)
      for idx,line_tup in enumerate(result_query) :
        #position_text = str(idx)+'.0'
        # convert tuple to string 
        #tup_str = ''
        tup_list = list()
        for item in line_tup:
          #tup_str = tup_str + str(item).ljust(20)
          tup_list.append(str(item))
          #print(tup_list)
          
        #print(tup_list[0],tup_list[2])
        self.fk_dbTreeView.insert('', tk.END, values=tup_list ,iid = idx )
 #       if int(tup_list[2]) > 0 :
#             self.fk_dbTreeView.move(idx, idx-1,idx)



# this window will open to choose dialog params if the query needs it
# for example the budget query .the transaction by dialog param . etc
#  it will open whenever the param in the query has dialog_param_1 ...

class InputDataWindow(tk.Tk) :


  
#  def __init__(self,parentWindowObj,dialog_param_list):
  def __init__(self,applQuery1,dialog_param_list):
      #print("HI")
      #self.master = master
     # self.parentWindowObj = parentWindowObj
      self.applQuery1 = applQuery1
     # newWindow = tk.Toplevel(parentWindowObj.master)
      self.newWindow = tk.Toplevel()
      self.newWindow.title("Input Data  Window")
   # sets the geometry of toplevel
      self.newWindow.geometry("400x700")
    # set this window as modal
    # we get the dialog param names 
      self.dialog_param_list = dialog_param_list
      # we get the dialog param history vlaues - if there is no vlaues then the list is empty 
      self.args_history_list = applQuery1.get_args_history_list_for_param('dialog')
      self.entry_list = list()
      self.button_list = list()

      today = date.today()
      date_from = today - timedelta(days=60)


      for idx,param_value in enumerate(self.dialog_param_list ):
         input1_label  = Label(self.newWindow,text=param_value) 
         self.entry_list.append(Entry(self.newWindow))
         # we check to see if we have history values 
         if len(self.args_history_list ) > 0 :
             # if yes then we enter the initail value from the arg history
             self.entry_list[idx].insert(0,self.args_history_list[idx])
         
         input1_label.grid(column=0, row=idx, sticky=tk.W, padx=5, pady=5)
         self.entry_list[idx].grid(column=1, row=idx, sticky=tk.W, padx=5, pady=5)
         
      save_input_button =Button(self.newWindow,text="SAVE"  ,command= lambda : self.save_input())     
      save_input_button.grid(column=1, row=idx+2, sticky=tk.W, padx=5, pady=5)
      
      self.newWindow.grab_set()


  def save_input(self):
   # we simply go over all the entries and append it to the appquery args
   # this will enable the query to run with args 
      for i,entry in enumerate(self.entry_list ):
          if self.applQuery1.args is None :
             self.applQuery1.args = list()
             
          self.applQuery1.args.append(entry.get())


   # before we close the window we store the values of the dialog args
   # in the history table app_query_args_history

   # we get the dialog param list
      dialog_param_name_list = self.applQuery1.get_param_name_list_for_param('dialog')
     # we create list for the sql args  
      sql_args = list ()
      sql_args.append(self.applQuery1.get_query_id())
      # we delete the old values 
      result_query,cursor_desc1 = execute_sql_no_except(mydbcon, "delete from appl_query_args_history where query_id  = ?",sql_args)
      # we insert the new values
      for idx ,param_name in enumerate(dialog_param_name_list):
          sql_args = [self.applQuery1.get_query_id(),param_name,self.applQuery1.args[idx]]
         # print (sql_args)
          result_query,cursor_desc1 = execute_sql_no_except(mydbcon, "insert into  appl_query_args_history ( query_id,param_name,arg_value) \
              values (?,?,? )",sql_args)

      result_query,cursor_desc1 = execute_sql_no_except(mydbcon,"commit")
          #return result_query[0][0]
   

      self.newWindow.destroy()


      
     # self.parentWindowObj.destroy()

  def date_peaker(self,idx) :
      #print('date peaker',idx)
      # Create Object
      root = Tk()
      # Set geometry
      root.geometry("400x400")
      # Add Calendar
      today = date.today()
      #print (today.strftime('%Y'),today.strftime('%m'),today.strftime('%d'))
      cal = Calendar(root, selectmode = 'day',date_pattern ='y-mm-dd',
        year = int(today.strftime('%Y')), month = int(today.strftime('%m')),
        day = int(today.strftime('%d')))
      cal.pack(pady = 20)

      def grad_date():
        #print ( cal.get_date())
        self.entry_list[idx].delete(0,END)
        self.entry_list[idx].insert(0,cal.get_date())
        root.destroy() 

    # Add Button and Label
      Button(root, text = "Get Date",command = grad_date).pack(pady = 20)



##--------------------------------   class LoadWindow   ------------------------
##------------------------------------------------------------------------------


class LoadWindow(tk.Tk):

    #mainWindowObj = None
    
    def __init__(self,mainWindowObj):
      #self.master = master
      self.mainWindowObj = mainWindowObj
      self.newWindow = tk.Toplevel(mainWindowObj.master)
      self.newWindow.title("Load Window")
 
    # sets the geometry of toplevel
      self.newWindow.geometry("800x700")
    # set this window as modal 
      self.newWindow.grab_set()
 
      header_frame = Frame(self.newWindow)
    # load buttons combo and entries
    #  load_data_frame = Frame(header_frame)     

      load_type_label = Label(header_frame,text="load_type ",font=f_labels)
     # self.width_length = tk.StringVar(value='100') 
     # exe_col_width_entry = Entry(header_frame,textvariable=self.width_length)
      file_name_label = Label(header_frame,text="file_name ",font=f_labels)
      load_button = Button(self.newWindow,text="Load data",command= lambda :self.load_dataframe_to_stage(),font=f_button)
      #backup_button = Button(self.newWindow,text="EXP_BACKUP ",command=self.say_hi(),font=f_button)
      self.load_var = tk.StringVar()
      self.load_combobox = ttk.Combobox(header_frame, textvariable=self.load_var,font=f_button)
      result_query,cursor_desc = execute_sql_no_except(mydbcon,"select key1 from app_params where key2 = 'SRC_ACCOUNT'" )
      #self.load_combobox['values'] = ['Max Card','Visa Card','IsraCard','Yahav','Discount','File']
      self.load_combobox['values'] = result_query
       # prevent typing a valu
      #load_combobox['state'] = 'readonly'
      self.load_combobox.current()
      self.load_combobox.bind("<<ComboboxSelected>>", lambda x: self.load_choose_combobox())
      load_file_button = Button(header_frame,text="Load File ",command=lambda :self.load_choose_file(),font=f_button)
      load_file_name = tk.StringVar()
      self.load_file_entry = Entry(header_frame,textvariable=load_file_name)
      save_config_button = Button(header_frame,text="save_config",font=f_button,command=lambda :self.load_save_config())
      
      debit_date_label = Label(header_frame,text="Debit Date",font=f_labels)
      self.load_debit_date = tk.StringVar(value='15/01/2022')
      self.load_debit_date_entry = Entry(header_frame,textvariable=self.load_debit_date)

      keyword_label = Label(header_frame,text="Key Word",font=f_labels)
      self.keyword_entry = Entry(header_frame)
      keyword_test_button =  Button(header_frame,text="test_data",font=f_button,command=lambda :self.check_columns_mapping())
      
      source_account_label = Label(header_frame,text="source account id",font=f_labels)
      self.source_account_entry = Entry(header_frame)

      date_format_label = Label(header_frame,text="date format",font=f_labels)
      self.date_format_entry = Entry(header_frame)
      
      load_type_label.grid(column=0, row=0, sticky=tk.W, padx=5, pady=5)
      self.load_combobox.grid(column=1, row=0, sticky=tk.W, padx=5, pady=5)
      save_config_button.grid(column=2, row=0, sticky=tk.W, padx=5, pady=5)


      source_account_label.grid(column=0, row=1, sticky=tk.W, padx=5, pady=5)
      self.source_account_entry.grid(column=1, row=1, sticky=tk.W, padx=5, pady=5)
      
      date_format_label.grid(column=0, row=2, sticky=tk.W, padx=5, pady=5)
      self.date_format_entry.grid(column=1, row=2, sticky=tk.W, padx=5, pady=5)

      debit_date_label.grid(column=0, row=3, sticky=tk.W, padx=5, pady=5)
      self.load_debit_date_entry.grid(column=1, row=3, sticky=tk.W, padx=5, pady=5)

      keyword_label.grid(column=0, row=4, sticky=tk.W, padx=5, pady=5)
      self.keyword_entry.grid(column=1, row=4, sticky=tk.W, padx=5, pady=5)
      
      file_name_label.grid(column=0, row=5, sticky=tk.W, padx=5, pady=5)
      self.load_file_entry.grid(column=1, row=5, sticky=tk.W, padx=5, pady=5)
      load_file_button.grid(column=2, row=5, sticky=tk.W, padx=5, pady=5)
      keyword_test_button.grid(column=2, row=6)






      end_header_label = Label(header_frame,text="____________",font=f_labels)
      end_header_label.grid(column=1,row=7)
      #backup_button.grid(column=0, row=8, sticky=tk.W, padx=5, pady=5)
      #load_data_frame.grid(column=2, row=0)
     # exe_col_width_entry.grid(column=0, row=7, sticky=tk.W, padx=5, pady=5)
      header_frame.grid(column=0, row=0)


      
      table_frame = Frame(self.newWindow)
      source_frame = Frame(self.newWindow)
      
      source_tree_columns=["sign","number","column"]
      self.source_TreeView = ttk.Treeview(source_frame,columns=source_tree_columns,show='headings')
      self.source_TreeView.bind("<Double-1>", self.onSourceDoubleclick)
      for header in source_tree_columns:
            self.source_TreeView.column(header, minwidth=0,anchor=CENTER, stretch=NO, width=100)
            self.source_TreeView.heading(header, text=header,anchor=CENTER)

      self.source_TreeView.grid(  column=0, row=3)  

      table_tree_columns=["column","number"]
      self.table_TreeView = ttk.Treeview(table_frame,columns=table_tree_columns,show='headings')
      for header in table_tree_columns:
            self.table_TreeView.column(header, minwidth=0,anchor=CENTER, stretch=NO, width=100)
            self.table_TreeView.heading(header, text=header,anchor=CENTER)
                 
      self.table_TreeView.grid(  column=0, row=3)  

      self.colors = ["red", "green", "aliceblue", "lightgray", "white", "yellow", "orange", "pink", "grey", "wheat", "brown"] 
      for color in self.colors:
        self.source_TreeView.tag_configure(color, background=color)
        self.table_TreeView.tag_configure(color, background=color)

  
      table_frame.grid(column=0, row=1)
      source_frame.grid(column=1, row=1)
 
      load_button.grid(column=0, row=3, columnspan = 2, sticky=tk.W+tk.E, padx=5, pady=5)
      



    def say_hi(self):
        print("hello ~ !")
        #self.result_scrolled_text.insert('2.0', 'This is a Text widget demo')




    def load_choose_file (self):
        load_file = fd.askopenfilename(initialdir="G:\RON\מסמכים בנקאיים וקופות גמל משכורות")
        #print("load file")
        #print (load_file)
        self.load_file_entry.delete(0, END)
        self.load_file_entry.insert(0, load_file)

    def load_choose_combobox (self):
          
        self.keyword_entry.delete(0, END)
        self.load_debit_date_entry.delete(0, END)
        self.date_format_entry.delete(0, END)
        self.source_account_entry.delete(0, END)

        
        now = datetime.datetime.now()
        #print (self.load_combobox.get())
       # 'Max Card','Visa Card','IsraCard','Yahav','Discount','File'
        result_query,cursor_desc = execute_sql_no_except(mydbcon,"select value1 from app_params where param_type ='LOAD' and key2 ='KEYWORD' and key1 = '"
                                                         +self.load_combobox.get()+ "'"  )
        #print(result_query)
        self.keyword_entry.insert(0,str(result_query[0][0]))
        #print(result_query)


        result_query,cursor_desc = execute_sql_no_except(mydbcon,"select value1 from app_params where param_type ='LOAD' and key2 ='DATE_FORMAT' and key1 = '"
                                                         +self.load_combobox.get()+ "'"  )
        self.date_format_entry.insert(0, result_query[0][0])

        result_query,cursor_desc = execute_sql_no_except(mydbcon,"select value1 from app_params where param_type ='LOAD' and key2 ='DEBIT_DAY' and key1 = '"
                                                         +self.load_combobox.get()+ "'"  )
        if result_query[0][0] is not None:
          self.load_debit_date_entry.insert(0, now.strftime(result_query[0][0]+"/%m/%Y"))

        result_query,cursor_desc = execute_sql_no_except(mydbcon,"select value1 from app_params where param_type ='LOAD' and key2 ='SRC_ACCOUNT' and key1 = '"
                                                         +self.load_combobox.get()+ "'"  )
        self.source_account_entry.insert(0, now.strftime(result_query[0][0]))


##        if self.load_combobox.get() == 'Max Card' or self.load_combobox.get() == 'Visa Card'   :   
##          self.load_debit_date_entry.delete(0, END)
##          self.load_debit_date_entry.insert(0, now.strftime("15/%m/%Y"))
##        elif self.load_combobox.get() == 'IsraCard' :
##          self.load_debit_date_entry.delete(0, END)
##          self.load_debit_date_entry.insert(0, now.strftime("02/%m/%Y"))
##        elif self.load_combobox.get() == 'File':
##          self.load_debit_date_entry.delete(0, END)
##          self.load_debit_date_entry.insert(0, "<table name>")
##        else:
##          self.load_debit_date_entry.delete(0, END)
          
          
       #   self.mainWindowObj.log_scrolled_text.insert(END,now.strftime("%Y-%m-%d %H:%M:%S") + '



    def check_columns_mapping (self) :

        # clear all the items in the treview 
      for item in self.table_TreeView.get_children():
           self.table_TreeView.delete(item)

      for item in self.source_TreeView.get_children():
           self.source_TreeView.delete(item)

   #   src_account_id =6




      #print ('load_file_df_columns')

      # run the query that  retrieves the maping fields from app_params table
      
      filename = param_json["procedure_sql_dir"]+"load_field_maping.txt"
      f = open(filename, "r")
      query_text = f.read()
      query_text = query_text.replace('<key1>',self.load_combobox.get())
      #print(query_text)
      result_query_map,cursor_desc = execute_sql_no_except(mydbcon,query_text)
      #this query return result for example :  ('table_col ','dataframe col','comon number' ,sign )
      # the purpose is to show the filed mapping by color tags so the use can see how 
      # each column in the source data frame is maped to the table by number and by color
      
   
      

      query_text = "select * from transactions_stage where 1=2"
      table_columns = list()    
      result_query,cursor_desc = execute_sql_no_except(mydbcon,query_text)
        # populate the columns list of table transactions from the cursor description
      for idx ,head_text in enumerate(cursor_desc) :
            table_columns.append(head_text[0])
            line_list = [head_text[0],str(idx)]
            color_tag =''
            # check to see if this column in the table has amapping number
            # if yes then the line in the tree view will have color tag 
            for rec in result_query_map :
                if int(rec[2]) == idx :
                   color_tag =  self.colors[idx]
                 

                
            if color_tag != '' :
               self.table_TreeView.insert('', tk.END,id=idx,values=line_list,tag=color_tag)                   
            else :
               self.table_TreeView.insert('', tk.END,id=idx,values=line_list)

             
##             in remark - was done in order to initialise tha table columns to table app_params
               
##            query_text = "insert into app_params (param_type,key1,key2,key3) values ('LOAD','" \
##                +"TABLE" +  "','FIELD',"+"'"+head_text[0]+"')"
##            print(query_text)
##            result_query,cursor_desc = execute_sql_no_except(mydbcon,query_text)
##            
##            result_query,cursor_desc = execute_sql_no_except(mydbcon,'commit')   
     
      
      #date_format = '%d/%m/%Y'
      #default_debit_date ='01/01/2022'
      #key_word = 'תאריך עסקה'
      key_word = self.keyword_entry.get()
      
      #file_name = 'G:\\RON\\מסמכים בנקאיים וקופות גמל משכורות\\כרטיסי אשראי\\לאומי\\20220815.xlsx'
      file_name = self.load_file_entry.get()
      xls = pd.ExcelFile(file_name)


 
      
#    print(xls.sheet_names)
#   loop over each sheet 
      for sheet in xls.sheet_names :
         # open the sheet 
         data = pd.read_excel (file_name,sheet_name=sheet,header=0)
         df = pd.DataFrame(data)

            
         # check to find the keyword indicationg that this is  atransaction list
         # this result a list of lines and aboolean if the word is in this row for example :
         # 0 false
         # 1 false
         # 2 true
         
         temp =df.apply(lambda row: row.astype(str).str.contains(key_word).any(), axis=1)
         #print(temp)
         # j is the line counter where the key word was founf if it was found (i is True
         #then we can start to load data frams from this position 
         j = 0
         for i in temp :
            # if yes then open the data frame from this line +1  in oreder to get the wright columns 
            if  i :
                #  print (j,i)
                  data = pd.read_excel (file_name,sheet_name=sheet,header=j+1)
                  df = pd.DataFrame(data)
                  df = self.replace_df_columns_newlines(df)
                  #print(df)
                  #print(df.columns)


                  # change the debit amount to minus for the transaction to be relavant
                  #df['סכום חיוב'] = (-1)* df['סכום חיוב']
                  #load_transactions_from_dataframe (conn,df,date_format,src_account_id,field_map_dict,default_debit_date)
                #  print (df.columns)

                  for idx,col in enumerate(df.columns):
                     #self.result_dbTreeView.insert('', tk.END,id=idx ,values=tup_list,tag = ('income'))
                     # "sign","number","column"]
##                     query_text = "select value1,value2 from app_params where param_type='LOAD' and "+ \
##                                      "key1 = '"+self.load_combobox.get() +"' and key2 = 'FIELD' " + \
##                                      " and key3 = '" + col +"'"
##                     result_query,cursor_desc = execute_sql_no_except(mydbcon,query_text)
##
 

                  

                        color_tag =''
                        col_number =''
                        col_sign =''
                        for rec in result_query_map :
                            if rec[1] == col and rec[2] != '' :
                               #print (col,rec[2],rec[3])
                               col_number = rec[2]
                               #print(col_number)
                               color_tag =  self.colors[int(col_number)]
                               col_sign = rec[3]
                             
                        line_list=[col_sign,col_number,col]
                            
                        if color_tag != '' :
                           self.source_TreeView.insert('', tk.END,id=idx,values=line_list,tag=color_tag)                   
                        else :
                           self.source_TreeView.insert('', tk.END,id=idx,values=line_list)
                       
                        #line_list=["+",str(idx),col]
                         #line_list=[result_query[0][1],result_query[0][0],col]
                        #self.source_TreeView.insert('', tk.END,id=idx,values=line_list)
                         #
##                        query_text = "insert into app_params (param_type,key1,key2,key3) values ('LOAD','" \
##                           +self.load_combobox.get() +  "','FIELD',"+"'"+col+"')"
##                        print(query_text)
##                        result_query,cursor_desc = execute_sql_no_except(mydbcon,query_text)

                    
                  #result_query,cursor_desc = execute_sql_no_except(mydbcon,'commit')           
##                  print(df.columns)
##                  df.set_axis(new_col_names,axis="columns",inplace=True)
##                  print(df.columns)
            j =j+1          




    def load_dataframe_to_stage (self) :
#     try:
      #temp_message = self.check_load_status ()
      #messagebox.showinfo(title=" Load Status" , message=temp_message)
      #messagebox.showinfo(title=" Load Status" , message="OK this is the message ")
      #print('load_dataframe_to_stage')
       
     # delete all records from stage table 
      execute_sql(mydbcon, 'delete from transactions_stage')
      execute_sql(mydbcon, 'commit')

      key_word = self.keyword_entry.get()
      
      #file_name = 'G:\\RON\\מסמכים בנקאיים וקופות גמל משכורות\\כרטיסי אשראי\\לאומי\\20220815.xlsx'
      file_name = self.load_file_entry.get()
      xls = pd.ExcelFile(file_name)

      src_account_id =   self.source_account_entry.get()
      date_format = self.date_format_entry.get()
      default_debit_date = self.load_debit_date_entry.get()

      # run the query that  retrieves the maping fields from app_params table
      #this query return result for example :  ('table_col ','dataframe col','comon number' ,sign )
      
      filename = param_json["procedure_sql_dir"]+"load_all_tab_field_maping.txt"
      f = open(filename, "r")
      query_text = f.read()
      query_text = query_text.replace('<key1>',self.load_combobox.get())
      result_query_map,cursor_desc = execute_sql_no_except(mydbcon,query_text)

      #create an empty dictionary 
      field_map_dict = {}

      #create sign list to hold the columns which ,ust be converted to mius
      sign_solumn_list = list()
      #loop over the query result and add it to the dictionary
      # this is in order to get the same structure of the example below 
      for rec in   result_query_map   :
         if rec[1] is None :
            field_map_dict[rec[0]] =  ''
         else:
            field_map_dict[rec[0]] =  rec[1]
            if rec[3] =='-' :
                sign_solumn_list.append(rec[1])
                
            
   
 #     print(field_map_dict)
##      field_map_dict = {
##         #  'account_id':'',
##           'trans_date':'תאריך',
##          'trans_amount':'',
##          'trans_curr':'',
##          'debit_date':'יום ערך',
##          'debit_amount':"debit",
##          'trans_desc':'תיאור התנועה',
##          'balance':'balance',  
##          'remark1':'אסמכתה',
##          'remark2':'',
##          'label':'',
##          'ref_account_id':''}
      


      
#    print(xls.sheet_names)
#   loop over each sheet 
      for sheet in xls.sheet_names :
         # open the sheet 
         data = pd.read_excel (file_name,sheet_name=sheet,header=0)
         df = pd.DataFrame(data)
         df = self.replace_df_columns_newlines(df)

            
         # check to find the keyword indicationg that this is  atransaction list
         # this result a list of lines and aboolean if the word is in this row for example :
         # 0 false
         # 1 false
         # 2 true
         
         temp =df.apply(lambda row: row.astype(str).str.contains(key_word).any(), axis=1)
         #print(temp)
         # j is the line counter where the key word was founf if it was found (i is True
         #then we can start to load data frams from this position 
         j = 0
         for i in temp :
            #print(i)
            # if yes then open the data frame from this line +1  in oreder to get the wright columns 
            if  i :
                  #print (j,i)
                  data = pd.read_excel (file_name,sheet_name=sheet,header=j+1)
                  df = pd.DataFrame(data)
                  df = self.replace_df_columns_newlines(df)
                  #print(df)
                  # change the sign of the columns which required to be converted 
                  for col in sign_solumn_list :
                     #print("ron")
                     #print(col)
                     df[col] = (-1)* df[col] 
                  #print ("RON DEBUG0")
                  #print(df)
                  #print (df["סכום החיוב"])
                  # we have to deal with columns which are mapped to same column in
                  # the table like זכות חובה   in yahav
                  # this query returns the following fields for example :
                  # for yahav : debit_amount   
                  
                  filename = param_json["procedure_sql_dir"]+"find_how_many_multi_columns_mapings_exists.txt"
                  #print ("RON DEBUG1")
                  f = open(filename, "r")
                  query_text = f.read()
                  query_text = query_text.replace('<key1>',self.load_combobox.get())
                  #print ("RON DEBUG2")
                  result_query_map1,cursor_desc = execute_sql_no_except(mydbcon,query_text)
                  #print ("RON DEBUG")
                  for rec1 in   result_query_map1   :
                      #print(rec1[0])
                      # add new column to the data frame name <debit_amount> for yahav and inialize it with 0
                      df[rec1[0]] ="0"
                      
                      # now we have to identify this columns 
                      # the table like זכות חובה   in yahav
                      # this query returns the following fields for example :
                      # for yahav : debit_amount , "zchot" ( in hebrew) , 6  
                      #             debit_amount ," hova "( in hebrew ) , 6 
                      filename = param_json["procedure_sql_dir"]+"find_multi_columns_mapping.txt"
                      f = open(filename, "r")
                      query_text = f.read()
                      query_text = query_text.replace('<key1>',self.load_combobox.get())
                      query_text = query_text.replace('<key2>',rec1[0])
                      result_query_map2,cursor_desc = execute_sql_no_except(mydbcon,query_text)
                      # now i have to loop over all the records in the data frame and check the specific columns
                      # and add the correct value to the new column wich i created in  df  - rec1[0]
                      for index, row in df.iterrows():
                          
                          for rec2 in   result_query_map2 :
                            if df.at[index,rec2[1]] != 0 :
                                df.at[index,rec1[0]] = row[rec2[1]]
                      # after populating the new column
                      # i change the field mapping of the data frams
                      # now there is anew column in the df with the same name as
                      # as the column name and valued from both columns
                      #print (field_map_dict)
                      #print(rec[0])
                      field_map_dict[rec1[0]] =  rec1[0]        
                      #print (field_map_dict)
                      
                  self.load_transactions_from_dataframe (mydbcon,df,date_format,src_account_id,field_map_dict,default_debit_date)
                    #                                     conn    ,data_frame,date_format,src_account_id,field_map_dict,default_debit_date
                  
            j =j+1
            
      temp_message = self.check_load_status ()
      messagebox.showinfo(title=" Load Status" , message=temp_message)
#     except:
      #  now after the load is done we open apopup window  with the load summary
#      temp_message = self.check_load_status ()
#      messagebox.showinfo(title=" Load Status" , message=temp_message)


    def check_load_status(self):
      # this functions check the stage area and return statitics about the data
      # like hoe many record , min and max dates , sumdebit amount , ..
        temp_message = ''
        temp_query = "select count(*),sum(debit_amount),max(trans_date),min(trans_date) from transactions_stage"
        #args = ("LOAD",self.load_combobox.get())
        result_query,cursor_desc = execute_sql_no_except(mydbcon,temp_query)
        temp_message = temp_message+" Number of records in Stage : "+ str(result_query[0][0])  \
             + '\n' + " the sum of stage debit amount is : " + str(result_query[0][1])    \
             + '\n' + " max stage trans_date is : " + str(result_query[0][2])   \
             + '\n' + " min stage trans_date is : " + str(result_query[0][3])   \
        
        return temp_message
      

    def load_save_config(self)  :
        #print(self.load_combobox.get())

        # delete the undo data from app params table related to this key in the combo 
        temp_query = "delete from app_params_undo where param_type = ? and key1 = ?"
        args = ("LOAD",self.load_combobox.get())
#       print (args)
        result_query,cursor_desc = execute_sql_no_except(mydbcon,temp_query,args)


        # insert into the app_params_und othe existing values of the app_params
        temp_query = "insert into app_params_undo select * from app_params where param_type = ? and key1 = ? "
        args = ("LOAD",self.load_combobox.get())        
        result_query,cursor_desc = execute_sql_no_except(mydbcon,temp_query,args)


        temp_query = "delete from app_params where param_type = ? and key1 = ? and key2 != 'FIELD'"
        args = ("LOAD",self.load_combobox.get())
#       print (args)
        result_query,cursor_desc = execute_sql_no_except(mydbcon,temp_query,args)
        
        result_query,cursor_desc = execute_sql_no_except(mydbcon,"commit")



        # save the current config data
        temp_query = "insert into app_params (param_type,key1,key2,value1) values (?,?,?,?)"
        args = ("LOAD",self.load_combobox.get(),"SRC_ACCOUNT",self.source_account_entry.get())        
        result_query,cursor_desc = execute_sql_no_except(mydbcon,temp_query,args)

        args = ("LOAD",self.load_combobox.get(),"DEBIT_DAY",self.load_debit_date_entry.get()[:2] )      
        result_query,cursor_desc = execute_sql_no_except(mydbcon,temp_query,args)


        args = ("LOAD",self.load_combobox.get(),"DATE_FORMAT",self.date_format_entry.get() )      
        result_query,cursor_desc = execute_sql_no_except(mydbcon,temp_query,args)

        args = ("LOAD",self.load_combobox.get(),"KEYWORD",self.keyword_entry.get() )      
        result_query,cursor_desc = execute_sql_no_except(mydbcon,temp_query,args)
        
        result_query,cursor_desc = execute_sql_no_except(mydbcon,"commit")
        
        
        
        

    def onSourceDoubleclick(self,event):
        item = self.source_TreeView.selection()[0]
        #print("you clicked on", item)
        region = self.source_TreeView.identify("region", event.x, event.y)
        column = self.source_TreeView.identify("column", event.x, event.y)
        #print ("you clicked" ,region,column)

        self.editColWin = tk.Toplevel()
        self.editColWin.title("Edit Column")
        self.editColWin.geometry("300x300")

        self.editColWin.grab_set()
        
        sign_label = Label (self.editColWin,text="sign")
        self.sign_entry = Entry(self.editColWin)
        col_number_label = Label (self.editColWin,text="column number")
        self.col_number_entry = Entry(self.editColWin)
        save_col_number_button = Button(self.editColWin,text="Save ",command= lambda : self.save_col_number(),font=f_button)
        
        sign_label.grid(column=0, row=0, sticky=tk.W, padx=5, pady=5)
        col_number_label.grid(column=1, row=0, sticky=tk.W, padx=5, pady=5)
        self.sign_entry.grid(column=0, row=1, sticky=tk.W, padx=5, pady=5)
        self.col_number_entry.grid(column=1, row=1, sticky=tk.W, padx=5, pady=5)
        save_col_number_button.grid(column=1, row=2, sticky=tk.W, padx=5, pady=5)
        col_number_entry.focus()


    def save_col_number(self):
        #print("save col number")
        curItem = self.source_TreeView.focus()
        tree_view_line = self.source_TreeView.item(curItem)
        tree_view_data = self.source_TreeView.item(curItem).get('values')
        #print(tree_view_line)
        #print(self.source_TreeView.item(curItem).get('values'))
        tree_view_data[0]= self.sign_entry.get()
        tree_view_data[1]= self.col_number_entry.get()
        
        selected_item = self.source_TreeView.selection()[0]
        self.source_TreeView.item(selected_item,values=tree_view_data)

        temp_query = "delete from  app_params where param_type = ? and key1 = ? and key2 = ? and key3 = ?"
        args = ("LOAD",self.load_combobox.get(),"FIELD",tree_view_data[2])        
        result_query,cursor_desc = execute_sql_no_except(mydbcon,temp_query,args)

        temp_query = "insert into   app_params(param_type,key1,key2,key3,value1,value2) values (?,?,?,?,?,? )"
        args = ("LOAD",self.load_combobox.get(),"FIELD",tree_view_data[2],self.col_number_entry.get(),self.sign_entry.get())        
        result_query,cursor_desc = execute_sql_no_except(mydbcon,temp_query,args)

        result_query,cursor_desc = execute_sql_no_except(mydbcon,"commit")
        
        self.editColWin.destroy()

    def replace_df_columns_newlines(self,df):
        new_col_names = list()
        for col in df.columns :
            if "\n" in col :
               # print(col)
               # print ("contains")
                new_col=col.replace("\n"," ")
              #  print(new_col)
                new_col_names.append(new_col)
            else:
                new_col_names.append(col)

       # df.set_axis(new_col_names,axis="columns",inplace=True)
       # df.set_axis(new_col_names,axis="columns")
        df.columns= new_col_names
        print(df.columns)
        return df 

    def load_transactions_from_dataframe (self,conn,data_frame,date_format,src_account_id,field_map_dict,default_debit_date):
##      this fubction load transactions into DB from dataframe
##      it gets the following parameters :
##     1. conn  - connction type for db in the db tha table transactions must allrrady exists
##         data_frame - is the data with the columns and  rows whuch where priviosly loded from the file 
##     2 date_format  - date format in the file for example : yyyy/mm/dd  is %Y/%m/D
##                      dd-mm-yyyy  is %d-%m-%Y
##           the  required date format in the db is yyyy-mm-dd - this function will
##             handle this conversion according to the format soource 
##     
##     4. account_id - (will be convreted to account id ) the account source of the transactions
##     
##     6. field_map_dict - this is a python dictionay that will map every excel column to the
##         relevant data base column in table transactions
##    7. default_debit_date - if it not exists than a default must be provided in the format of the other dates in this file
##    8. data_frame_filer - string for pandas data frame to remove inepropriate data rows like "סה"כ"
              for index, row in data_frame.iterrows():
                  temp_list =[]
                  temp_rec =[]
                  for i in field_map_dict :
                    # check if the field is '' 
                    if field_map_dict[i] != '':

                      # for situations where the data is allready in format of datetime then it must converted back to string
                      #print(type(row[field_map_dict[i]]))
                      if  isinstance(row[field_map_dict[i]], str) or  isinstance(row[field_map_dict[i]],float) or  isinstance(row[field_map_dict[i]],int):
                        temp_rec.append(row[field_map_dict[i]])                                                 
                      else :                        
                        temp_rec.append(row[field_map_dict[i]].strftime( date_format))
                    else:
                      # check if '' is debit date for this field  we need to add default_debit _date 
                      if i == 'debit_date' :
                        temp_rec.append(default_debit_date) 
                      else :
                        temp_rec.append('')


                  temp_rec.insert(0,src_account_id)
                  # this try and exception are here due to records which i didnt manged to filter
                  # like סה"כ   and in the data frame there was not a genric filter for this
                  # this for example fai;s in th dtae conversion
                  print('ron1')
                  try :
                    print('ron2')
                    object2=TransRec(date_format,temp_rec[0],temp_rec[1],temp_rec[2],temp_rec[3],temp_rec[4],temp_rec[5],temp_rec[6],temp_rec[7],temp_rec[8],temp_rec[9],temp_rec[10],temp_rec[11])
                    #object2.display()
                    print('ron3')
                    object2.insertTransrecTodb( conn )
                  except Exception as e:
                    print (e)
                    print("error occurred during insert",temp_rec)
   




class TransRec :
    def __init__(self, date_format,src_account_id,trans_date,trans_amount,trans_curr,debit_date, debit_amount,trans_desc,balance,remark1,remark2,label,ref_account_id):
        # dates must be string in format 2022-02-23
        # if you wamt to convert date you cna use
        #  temp1 = parse(trans_date, dayfirst=True)
        #  temp1.strftime( "%Y-%m-%d")
        # or use date_rotation which is defined  in this program
        #print(date_format,account_id,trans_date,trans_amount,trans_curr,debit_date, debit_amount,trans_desc,balance,remark1,remark2,label)
            if date_format == '' :
              self.trans_date = trans_date
              self.debit_date = debit_date
            else :
              temp1 = datetime.datetime.strptime(trans_date, date_format)
              self.trans_date = temp1.strftime( "%Y-%m-%d")
              temp2 = datetime.datetime.strptime(debit_date, date_format)
              self.debit_date = temp2.strftime( "%Y-%m-%d")
              
            self.src_account_id = src_account_id
         #   self.trans_date = trans_date
            self.trans_amount = trans_amount
            self.trans_curr = trans_curr
         #   self.debit_date = debit_date
            self.debit_amount = debit_amount
            self.trans_desc = trans_desc
            self.balance = balance
            self.remark1 = remark1
            self.remark2 = remark2
            self.label = label
            self.ref_account_id = ref_account_id

         

    def listTransRec (self) :
       tempTrans=(self.src_account_id,self.trans_date,self.trans_amount,self.trans_curr,self.debit_date,self.debit_amount,self.trans_desc,self.balance,
                  self.remark1,self.remark2,self.label,self.ref_account_id )
       return tempTrans

    def display(self):
        print (self.listTransRec())
        
    def findIfDuplicate (self,conn):
        result =0
        try:
          temp_query = " select count(*) from transactions where account_id = ? and trans_date =? and debit_amount=? and trans_desc = ? and debit_date =? "
          args = (self.src_account_id,self.trans_date,self.debit_amount,self.trans_desc,self.debit_date)
#         print (args)
          cursor = conn.execute(temp_query, args)        
          result = cursor.fetchall()
#         print(result[0])
          return result[0][0]
        except Error as e:
          print(f"The error '{e}' occurred")


    def insertTransrecTodb(self,conn):
      """
      Create a new task
      :param conn:
       :param task:
      :return:
      """
      print("ron2")

      sql = ''' INSERT INTO transactions_stage(src_account_id,trans_date,trans_amount,trans_curr,debit_date, debit_amount,trans_desc,balance,remark1,remark2,label,ref_account_id)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?) '''
      print(sql)
      cur = conn.cursor()

      cur.execute(sql, self.listTransRec())
      conn.commit()

      return cur.lastrowid





def backup_db_to_excel (conn):
  #param_file = 'G:\\RON\\PHYTON\\PROGRAMS\\MonyManager\\PARAM\\moneymanagerparam.txt'
  #f = open(param_file, "r")
  #print(f.read())
  #json_param = json.loads(f.read())
  #print (json_param["backup_table_names"])
  now = datetime.datetime.now()
  #export file name 
  backup_file_name = param_json["backup_dir"]+param_json["exp_file_prefix"]+ now.strftime("%Y-%m-%d_%H-%M-%S")+".xlsx"
  # db data base file name 
  backup_db_file_name = param_json["backup_db_dir"]+"dbbackup_"+now.strftime("%Y-%m-%d_%H-%M-%S")+".db"
  #print (backup_file_name)

  # export tabkes to excel file 
 # with pd.ExcelWriter(backup_file_name, engine="xlsxwriter", options = {'strings_to_numbers': True, 'strings_to_formulas': False}) as writer:
  with pd.ExcelWriter(backup_file_name, engine="xlsxwriter" ) as writer:
        for table_name in param_json["backup_table_names"].split(",") :
            print(table_name)
            try:
                df = pd.read_sql("Select * from " + table_name, conn)
                df.to_excel(writer, sheet_name = table_name, header = True, index = False)
                print("File saved successfully!")
            except:
                print("There is an error")

  # copy sqlite db file to backup destination
  print(param_json["db_file_name"],backup_db_file_name)
  copyfile(param_json["db_file_name"],backup_db_file_name)
                
##  json_param = 
##        f = open(filename, "r")
##        self.query_scrolled_text.insert('1.0',f.read())
##        temp_text = self.query_scrolled_text.get('1.0',END)
##        if "TABLE" in temp_text :
##              print ("YES")
##              a = temp_text.find('>')
##              b = temp_text.find('<')
##              print(temp_text[a+1:b])              

## ----------------------------  start Gui section -----------------------------

#global result_dbTreeView


class MyMlearning :
   def __init__(self, df):
       self.df = df 

   

   def tune_params_to_gradient_boost(self,X_train,y_train):
  # Define the parameter grid to search
##    param_grid = {
##     'n_estimators': [100,  300,  500],
##     'learning_rate': [0.01,  0.1, 0.2],
##     'max_depth': [3, 5,  9],
##     'min_samples_split': [2, 5, 10],
##     'min_samples_leaf': [1, 2, 4],
##     'subsample': [0.5, 1.0],
##     'max_features': ['auto', 'sqrt', 'log2'],
##     'loss':['squared_error', 'huber']
##    }

    param_grid = {
     'n_estimators': [  300,  500],
     'max_depth': [3, 5],
     'subsample': [0.5],
     'max_features': [ 'sqrt'],
     'loss':[ 'huber']
    }
  

    rf = ensemble.GradientBoostingRegressor()

    # Initialize GridSearchCV
    grid_search = GridSearchCV(estimator=rf, param_grid=param_grid, cv=2, n_jobs=-1, verbose=2)
    with joblib.parallel_backend('threading'):
       grid_search.fit(X_train, y_train)
    # Fit GridSearchCV
   # grid_search.fit(X_train, Y_train)

    # Get the best parameters and score
    best_parameters = grid_search.best_params_
    best_score = grid_search.best_score_
    print(f'Best Parameters: {best_parameters}')
    print(f'Best Score: {best_score}')
    return grid_search.best_params_

   def run_ml_model(self,model,X_train,X_test,y_train,y_test):
       # this function gets train and test data' it check the model
       # and return the best one of  them according to the data
       #avg_real =  y_test['sum_debit'].mean()
       avg_test = y_test['sum_debit'].mean()
       try :
           model.fit(X_train,y_train)
           y_test_predict = model.predict(X_test)
           mae = mean_absolute_error(y_test, y_test_predict)
           
##      mae = mean_absolute_error(y_test, y_test_predict)
##      avg_test = df_all['sum_debit'].mean()
##
##      print ("mae is:", mae)
##      print ("avg_test is:", avg_test)
##      print ("percent_error is :", 100*mae/abs(avg_test))
           
       #    print ( type(model).__name__ )
       #    print ('----------mean_absolute_error:'+str(mae))
       #    print ('----------mean_value:'+str(avg_test))
       #    print ('-----------error percent :'+str(100*mae/avg_test) )
       except :
           #print(f"Error Message: {e}")
           model = None
           #create huge error - this will exclude this model from the best performance  models 
           mae = 9999999999

##mae = mean_absolute_error(y_test, y_test_predict)
##avg_test = df_all['sum_debit'].mean()
##
##print ("mae is:", mae)
##print ("avg_test is:", avg_test)
##print ("percent_error is :", 100*mae/abs(avg_test))
           
       predict_error_percent = abs(100*mae/avg_test)
       
       return model , predict_error_percent ,mae  

   def run_parent_key_prediction_with_GradientBoost(self,month_to_predit):
      # get all the parent labels into  a data_frame 
      all_parent_labels_df = pd.read_sql_query (
         " select  iif (l.level > 1 , substr(t.label,0,length(substr(t.label,0,instr(t.label,'\\')))+instr(substr(t.label,instr(t.label,'\\')+1),'\\')+1),t.label) parent_label, \
            strftime('%Y-%m',max(debit_date)) max_date  \
           from transactions t  \
            join labels l  \
            on t.label= l.label \
           where ifnull(ref_account_id,0) not in (select account_id from accounts where account_type ='כרטיס אשראי') \
            group by  iif (l.level > 1 , substr(t.label,0,length(substr(t.label,0,instr(t.label,'\\')))+instr(substr(t.label,instr(t.label,'\\')+1),'\\')+1),t.label) \
          ",mydbcon)

#            and t.label = 'הכנסה\משכורת רון' \
#     in format 2025_06
      max_date = all_parent_labels_df['max_date'].max() 

      # add columns to the main data frame in order to populate it later
      # num_rows - how many rows are there for this parent_label
      all_parent_labels_df["num_rows"] = None
      # avg_last_12_month - the avg of last 12 month
      all_parent_labels_df["mean_12_month"] = None      
       # predict_type - iif it was done using avg 12 motnh or with prediction ml
      all_parent_labels_df["predict_type"] = None
       # predict_remark - to populate valus such as which model type and accuracy 
      all_parent_labels_df["predict_remark"] = None
      # predict error percent
      all_parent_labels_df["predict_error_percent"] = None
      # predict_value - the value of the prediction 
      all_parent_labels_df["predict_value"] = None
      # loop over the data frame to get each parent lable value 
      for i in range(len(all_parent_labels_df)):
          parent_label_name = all_parent_labels_df.iloc[i]['parent_label']
          month_date_str = month_to_predit+'-01'
          query_args = [month_date_str,parent_label_name]      

          single_parent_labels_df = pd.read_sql_query("select  t1.debit_month_str  ,t2.debit_year,t2.debit_month, t2.sum_debit\
               from  \
               ( select distinct(strftime('%Y-%m',debit_date)) debit_month_str \
               from transactions  where debit_date < ?)    t1 \
               left outer join \
               ( select strftime('%Y-%m',debit_date)  debit_month_str,strftime('%Y',debit_date) debit_year,strftime('%m',debit_date) debit_month,sum(debit_amount) sum_debit \
                           from transactions t  \
                           where   instr(t.label,?) > 0   \
                           and ifnull(ref_account_id,0) not in (select account_id from accounts where account_type ='כרטיס אשראי') \
                           group by strftime('%Y-%m',debit_date) ,strftime('%Y',debit_date) ,strftime('%m',debit_date)   \
                           order by debit_date  ) t2 \
               on(t1.debit_month_str = t2.debit_month_str) \
               order by t1.debit_month_str ",mydbcon,params=query_args)    

             # first we convert all nulls to 0 in the dataframe 
          single_parent_labels_df = single_parent_labels_df.fillna(0)
             # count how many records in this data frame          
          count_single_df_rows = single_parent_labels_df.shape[0]
             #count how many zeros in the sum_ebit column
          zero_count = zero_count = (single_parent_labels_df['sum_debit'] == 0).sum()

          all_parent_labels_df.loc[i, 'num_rows'] = count_single_df_rows - zero_count
             # calc the amount of train and test values 
          train_counter = int(0.7*count_single_df_rows)          
         # print(single_parent_labels_df)
          # calc the mean value of the last 12 month
          mean_12_month = single_parent_labels_df.loc[(count_single_df_rows-12):,['sum_debit']].mean()
          all_parent_labels_df.loc[i, 'mean_12_month'] = float(mean_12_month)
         # print(single_parent_labels_df)

   # we decalre flag_no_ml to see if we managed to predict - if we can then we use it
   # if no we will put avg 12 value
          flag_no_ml = 1  
          if (zero_count*100/ count_single_df_rows ) < 30 :
             #we will prepare the train and test values

             # now we  prepare the train and test values
              X_train = single_parent_labels_df.loc[0:train_counter-1, ['debit_year', 'debit_month']]
              y_train = single_parent_labels_df.loc[0:train_counter-1, ['sum_debit']]
              X_test =  single_parent_labels_df.loc[train_counter:, ['debit_year', 'debit_month']]
              y_test =  single_parent_labels_df.loc[train_counter:, ['sum_debit']]

              
              best_params = self.tune_params_to_gradient_boost(X_train,y_train)
              
              model = ensemble.GradientBoostingRegressor(
                   n_estimators=best_params['n_estimators'],
                   max_depth=best_params['max_depth'],
                   subsample=best_params['subsample'],
                   max_features=best_params['max_features'],
                   loss=best_params['loss']
                    )

              temp_result_model,error_percent,mae = self.run_ml_model(model,X_train,X_test,y_train,y_test)

    
              if error_percent < 20 :
                    # we managed to find approprate ml modek to use 
                  flag_no_ml =0
                  all_parent_labels_df.loc[i, 'predict_type'] = 'ml'
                  all_parent_labels_df.loc[i, 'predict_error_percent'] = error_percent
                  all_parent_labels_df.loc[i, 'predict_remark'] = type(temp_result_model).__name__
                  # we  prepare the value to predict
                  data =  {'debit_year': [int(month_to_predit[0:4])],
                             'debit_month': [int(month_to_predit[5:6])]}
                  x_predit_df = pd.DataFrame(data)
                  y_predict_df =temp_result_model.predict(x_predit_df)
                 # print(y_predict_df)
                  predict_value = y_predict_df[0]
                  all_parent_labels_df.loc[i, 'predict_value'] = predict_value



          if flag_no_ml ==1  :
             all_parent_labels_df.loc[i, 'predict_type'] = 'avg_12_month'
             all_parent_labels_df.loc[i, 'predict_remark'] = 'none'
             all_parent_labels_df.loc[i, 'predict_value'] = float(mean_12_month)
             
      return all_parent_labels_df
    #  print(all_parent_labels_df[['parent_label','num_rows','predict_value']] )
    #  print(all_parent_labels_df )
                  
# this function gets month to predict in format - "2025_07" and tries to predict it
   # month to predict is in format "2025-06"
   def run_parent_key_prediction (self,month_to_predit):

      # get all the parent labels into  a data_frame 
      all_parent_labels_df = pd.read_sql_query (
         " select  iif (l.level > 1 , substr(t.label,0,length(substr(t.label,0,instr(t.label,'\\')))+instr(substr(t.label,instr(t.label,'\\')+1),'\\')+1),t.label) parent_label, \
            strftime('%Y-%m',max(debit_date)) max_date  \
           from transactions t  \
            join labels l  \
            on t.label= l.label \
           where ifnull(ref_account_id,0) not in (select account_id from accounts where account_type ='כרטיס אשראי') \
            group by  iif (l.level > 1 , substr(t.label,0,length(substr(t.label,0,instr(t.label,'\\')))+instr(substr(t.label,instr(t.label,'\\')+1),'\\')+1),t.label) \
          ",mydbcon)

#            and t.label = 'הכנסה\משכורת רון' \
#     in format 2025_06
      max_date = all_parent_labels_df['max_date'].max() 

      # add columns to the main data frame in order to populate it later
      # num_rows - how many rows are there for this parent_label
      all_parent_labels_df["num_rows"] = None
      # avg_last_12_month - the avg of last 12 month
      all_parent_labels_df["mean_12_month"] = None      
       # predict_type - iif it was done using avg 12 motnh or with prediction ml
      all_parent_labels_df["predict_type"] = None
       # predict_remark - to populate valus such as which model type and accuracy 
      all_parent_labels_df["predict_remark"] = None
      # predict_value - the value of the prediction 
      all_parent_labels_df["predict_value"] = None
      # loop over the data frame to get each parent lable value 
      for i in range(len(all_parent_labels_df)):
          parent_label_name = all_parent_labels_df.iloc[i]['parent_label']
          month_date_str = month_to_predit+'-01'
          query_args = [month_date_str,parent_label_name]
         # print(parent_label_name)
          #print(query_args)
          
      
          single_parent_labels_df = pd.read_sql_query("select  t1.debit_month_str  ,t2.debit_year,t2.debit_month, t2.sum_debit\
               from  \
               ( select distinct(strftime('%Y-%m',debit_date)) debit_month_str \
               from transactions  where debit_date < ?)    t1 \
               left outer join \
               ( select strftime('%Y-%m',debit_date)  debit_month_str,strftime('%Y',debit_date) debit_year,strftime('%m',debit_date) debit_month,sum(debit_amount) sum_debit \
                           from transactions t  \
                           where   instr(t.label,?) > 0   \
                           and ifnull(ref_account_id,0) not in (select account_id from accounts where account_type ='כרטיס אשראי') \
                           group by strftime('%Y-%m',debit_date) ,strftime('%Y',debit_date) ,strftime('%m',debit_date)   \
                           order by debit_date  ) t2 \
               on(t1.debit_month_str = t2.debit_month_str) \
               order by t1.debit_month_str ",mydbcon,params=query_args)    

             # first we convert all nulls to 0 in the dataframe 
          single_parent_labels_df = single_parent_labels_df.fillna(0)
             # count how many records in this data frame          
          count_single_df_rows = single_parent_labels_df.shape[0]
             #count how many zeros in the sum_ebit column
          zero_count = zero_count = (single_parent_labels_df['sum_debit'] == 0).sum()

          all_parent_labels_df.loc[i, 'num_rows'] = count_single_df_rows - zero_count
             # calc the amount of train and test values 
          train_counter = int(0.7*count_single_df_rows)          
         # print(single_parent_labels_df)
          # calc the mean value of the last 12 month
          mean_12_month = single_parent_labels_df.loc[(count_single_df_rows-12):,['sum_debit']].mean()
          all_parent_labels_df.loc[i, 'mean_12_month'] = float(mean_12_month)
         # print(single_parent_labels_df)

         # 

          
         # print(count_single_df_rows,zero_count,train_counter)
         
         # we decalre flag_no_ml to see if we managed to predict - if we can then we use it
         # if no we will put avg 12 value
          flag_no_ml = 1          
          #check if there are more then 70% values so we can start prediction
          if (zero_count*100/ count_single_df_rows ) < 30 :
             #we will prepare the train and test values

             # now we  prepare the train and test values
              X_train = single_parent_labels_df.loc[0:train_counter-1, ['debit_year', 'debit_month']]
              y_train = single_parent_labels_df.loc[0:train_counter-1, ['sum_debit']]
              X_test =  single_parent_labels_df.loc[train_counter:, ['debit_year', 'debit_month']]
              y_test =  single_parent_labels_df.loc[train_counter:, ['sum_debit']]
             # declare list of 6 models tha we will use to predict 
              model_list = [ensemble.GradientBoostingRegressor(),
               linear_model.LinearRegression(),
               linear_model.Ridge(),
               linear_model.Lasso(),
               linear_model.ElasticNet(),
               linear_model.HuberRegressor()]
           #  loop over the models and run it for

              for model in model_list :
                temp_result_model,error_percent,mae = self.run_ml_model(model,X_train,X_test,y_train,y_test)
                #print(type(temp_result_model).__name__,error_percent)
                if error_percent < 20 :
                    # we managed to find approprate ml modek to use 
                  flag_no_ml =0
                  all_parent_labels_df.loc[i, 'predict_type'] = 'ml'
                  all_parent_labels_df.loc[i, 'predict_remark'] = type(temp_result_model).__name__
                  # we  prepare the value to predict
                  data =  {'debit_year': [int(month_to_predit[0:4])],
                             'debit_month': [int(month_to_predit[5:6])]}
                  x_predit_df = pd.DataFrame(data)
                  y_predict_df =temp_result_model.predict(x_predit_df)
                 # print(y_predict_df)
                  predict_value = y_predict_df[0]
                  all_parent_labels_df.loc[i, 'predict_value'] = predict_value

          if flag_no_ml ==1  :
             all_parent_labels_df.loc[i, 'predict_type'] = 'avg_12_month'
             all_parent_labels_df.loc[i, 'predict_remark'] = 'none'
             all_parent_labels_df.loc[i, 'predict_value'] = float(mean_12_month)
             
      return all_parent_labels_df

  # this procedure get month ant check its lable clasification using all
  #  the data which is available from the transactions table
  
   def predict_label_for_transaction  (self,month_to_predit) :
   
      # get all the transactions data except the data of this month
      query_args = [month_to_predit]
      transactions_df = pd.read_sql_query (
         " select src_account_id,trans_date,trans_amount,debit_date,debit_amount,\
           trans_desc ,label  \
           from transactions  \
           where trans_desc is not null \
           and trans_desc >'2018-01-01' \
           and instr(debit_date,?) = 0  \
           order by debit_date   "     
          ,mydbcon,params=query_args)

      # Handle missing values in 'trans_amount' by filling with the mean
      #transactions_df['trans_amount'] = transactions_df['trans_amount'].fillna(transactions_df['trans_amount'].mean())


     # Convert date columns to datetime objects
      transactions_df['trans_date'] = pd.to_datetime(transactions_df['trans_date'])
      transactions_df['debit_date'] = pd.to_datetime(transactions_df['debit_date'])     

     # Extract new features from date columns (month and day of week)
      transactions_df['trans_month'] = transactions_df['trans_date'].dt.month
      transactions_df['trans_dayofweek'] = transactions_df['trans_date'].dt.dayofweek
      transactions_df['trans_dayofmonth'] = transactions_df['trans_date'].dt.day
      transactions_df['debit_month'] = transactions_df['debit_date'].dt.month
      transactions_df['debit_dayofweek'] = transactions_df['debit_date'].dt.dayofweek
      transactions_df['debit_dayofmonth'] = transactions_df['debit_date'].dt.day

    # Drop the original date columns now that we've extracted features from them
      transactions_df = transactions_df.drop(columns=['trans_date', 'debit_date'])

 
    # Define features (X) and target (y)
      X = transactions_df.drop('label', axis=1)
      y = transactions_df['label']

      # Split the data into training and testing sets.
      # Note: stratify is not used here due to the small, imbalanced sample,
      # but it is recommended for larger datasets.
      X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

      # Define preprocessing for different types of features
      preprocessor = ColumnTransformer(
          transformers=[
              ('text', TfidfVectorizer(), 'trans_desc'),
              ('numeric', StandardScaler(), ['trans_amount', 'debit_amount', 'src_account_id', \
                                             'trans_month', 'trans_dayofweek','trans_dayofmonth',  \
                                             'debit_month', 'debit_dayofweek','debit_dayofmonth'])
          ],
          remainder='passthrough'
      )

      # Create the machine learning pipeline with a Logistic Regression classifier
      model = Pipeline(steps=[
          ('preprocessor', preprocessor),
          ('classifier',LogisticRegression(  random_state=42))
      ])

##      model = Pipeline(steps=[
##          ('preprocessor', preprocessor),
##          ('classifier', RandomForestClassifier( random_state=42))
##      ])

      # Define the model within the pipeline
##      pipeline_model = Pipeline(steps=[
##          ('preprocessor', preprocessor),
##          ('classifier', RandomForestClassifier(random_state=42))
##      ])
##      
##
##      param_grid = {
##       'classifier__n_estimators': [100, 200, 300],
##       'classifier__max_depth': [10, 20, None],
##       'classifier__min_samples_split': [2, 5, 10] }

       # Set up the grid search
##      grid_search = GridSearchCV(pipeline_model, param_grid, cv=5, scoring='accuracy')
##
##      # Fit the grid search to your data
##      grid_search.fit(X_train, y_train)
##
##      # The best model is now available at grid_search.best_estimator_
##      best_model = grid_search.best_estimator_     
     

##              model_list = [ensemble.GradientBoostingRegressor(),
##               linear_model.LinearRegression(),
##               linear_model.Ridge(),
##               linear_model.Lasso(),
##               linear_model.ElasticNet(),
##               linear_model.HuberRegressor()]

#      model = best_model

      # Train the model
      model.fit(X_train, y_train)

      # Predict on the test data
      y_pred = model.predict(X_test)

      # Evaluate the model
    #  print("Model Evaluation Report:")
    #  print(classification_report(y_test, y_pred, zero_division=0))

      # now i get the required values of this month
      query_args = [month_to_predit]
      transactions_df = pd.read_sql_query (
         " select trans_id,src_account_id,trans_date,trans_amount,debit_date,debit_amount,\
           trans_desc ,label  \
           from transactions  \
           where trans_desc is not null \
            and instr(debit_date,?) > 0  \
            order by trans_id   "     
          ,mydbcon,params=query_args)


     # save the trans_id columns for the final output
      trans_id_df = transactions_df[['trans_id','debit_date']]

     # drop trans_id from the transactions_df before the model run
     
      transactions_df = transactions_df.drop(columns=['trans_id'])

     
     # Convert date columns to datetime objects
      transactions_df['trans_date'] = pd.to_datetime(transactions_df['trans_date'])
      transactions_df['debit_date'] = pd.to_datetime(transactions_df['debit_date'])     

     # Extract new features from date columns (month and day of week)
      transactions_df['trans_month'] = transactions_df['trans_date'].dt.month
      transactions_df['trans_dayofweek'] = transactions_df['trans_date'].dt.dayofweek
      transactions_df['trans_dayofmonth'] = transactions_df['trans_date'].dt.day
      transactions_df['debit_month'] = transactions_df['debit_date'].dt.month
      transactions_df['debit_dayofweek'] = transactions_df['debit_date'].dt.dayofweek
      transactions_df['debit_dayofmonth'] = transactions_df['debit_date'].dt.day

    # Drop the original date columns now that we've extracted features from them
      transactions_df = transactions_df.drop(columns=['trans_date', 'debit_date'])

    # Define features (X) and target (y)
      X = transactions_df.drop('label', axis=1)
      y = transactions_df['label']     


      # Predict on the test data
      y_pred_new = model.predict(X)

      result_df = pd.concat([trans_id_df, X], axis=1)
      result_df['label'] = y
      result_df['predict_label'] = y_pred_new
      result_df = result_df.drop(columns=['trans_dayofweek', 'trans_dayofmonth','debit_dayofweek', 'debit_dayofmonth','trans_month','debit_month'])
      result_df['eq_lables'] = np.where(result_df['label'] == result_df['predict_label'], 0, -1)
      
      
      
      #print(  result_df)

      return result_df


   def predit_parent_label_debit_new(self,month_to_predit):
      # get all the parent labels into  a data_frame 
      all_parent_labels_df = pd.read_sql_query (
         " select  iif (l.level > 1 , substr(t.label,0,length(substr(t.label,0,instr(t.label,'\')))+instr(substr(t.label,instr(t.label,'\')+1),'\')+1),t.label) parent_label, \
            strftime('%Y-%m',max(debit_date)) max_date  \
           from transactions t  \
            join labels l  \
            on t.label= l.label \
           where ifnull(ref_account_id,0) not in (select account_id from accounts where account_type ='כרטיס אשראי') \
            group by  iif (l.level > 1 , substr(t.label,0,length(substr(t.label,0,instr(t.label,'\')))+instr(substr(t.label,instr(t.label,'\')+1),'\')+1),t.label) \
          ",mydbcon)

     # add columns to the main data frame in order to populate it later
      # num_rows - how many rows are there for this parent_label
      all_parent_labels_df["num_rows"] = None
      # avg_last_12_month - the avg of last 12 month
      all_parent_labels_df["mean_12_month"] = None      
       # predict_type - iif it was done using avg 12 motnh or with prediction ml
      all_parent_labels_df["predict_type"] = None
       # predict_remark - to populate valus such as which model type and accuracy 
      all_parent_labels_df["predict_remark"] = None
      # predict error percent
      all_parent_labels_df["predict_error_percent"] = None
      # predict_value - the value of the prediction 
      all_parent_labels_df["predict_value"] = None
      # loop over the data frame to get each parent lable value 
  

      for i in range(len(all_parent_labels_df)):
          parent_label_name = all_parent_labels_df.iloc[i]['parent_label']
          month_date_str = month_to_predit+'-01'
          query_args = [month_date_str,parent_label_name]      

          single_parent_labels_df = pd.read_sql_query("select  t1.debit_month_str  ,t2.debit_year,t2.debit_month, t2.sum_debit\
               from  \
               ( select distinct(strftime('%Y-%m',debit_date)) debit_month_str \
               from transactions  where debit_date < ?)    t1 \
               left outer join \
               ( select strftime('%Y-%m',debit_date)  debit_month_str,strftime('%Y',debit_date) debit_year,strftime('%m',debit_date) debit_month,sum(debit_amount) sum_debit \
                           from transactions t  \
                           where   instr(t.label,?) > 0   \
                           and ifnull(ref_account_id,0) not in (select account_id from accounts where account_type ='כרטיס אשראי') \
                           group by strftime('%Y-%m',debit_date) ,strftime('%Y',debit_date) ,strftime('%m',debit_date)   \
                           order by debit_date  ) t2 \
               on(t1.debit_month_str = t2.debit_month_str) \
               order by t1.debit_month_str ",mydbcon,params=query_args)    

             # first we convert all nulls to 0 in the dataframe 
          single_parent_labels_df = single_parent_labels_df.fillna(0)
             # count how many records in this data frame          
          count_single_df_rows = single_parent_labels_df.shape[0]
             #count how many zeros in the sum_ebit column
          zero_count = zero_count = (single_parent_labels_df['sum_debit'] == 0).sum()

          all_parent_labels_df.loc[i, 'num_rows'] = count_single_df_rows - zero_count
             # calc the amount of train and test values 
          train_counter = int(0.7*count_single_df_rows)          

          # calc the mean value of the last 12 month
          mean_12_month = single_parent_labels_df.loc[(count_single_df_rows-12):,['sum_debit']].mean()
          all_parent_labels_df.loc[i, 'mean_12_month'] = float(mean_12_month)  

          # we decalre flag_no_ml to see if we managed to predict - if we can then we use it
         # if no we will put avg 12 value
          flag_no_ml = 1          
          #check if there are more then 70% values so we can start prediction
          if (zero_count*100/ count_single_df_rows ) < 20 :
             #we will prepare the train and test values

             # now we  prepare the train and test values
              X_train = single_parent_labels_df.loc[0:train_counter-1, ['debit_year', 'debit_month']]
              y_train = single_parent_labels_df.loc[0:train_counter-1, ['sum_debit']]
              X_test =  single_parent_labels_df.loc[train_counter:, ['debit_year', 'debit_month']]
              y_test =  single_parent_labels_df.loc[train_counter:, ['sum_debit']]

              preprocessor = ColumnTransformer(
                transformers=[
                 ('numeric', StandardScaler(), ['debit_year', 'debit_month'])
                    ],
                remainder='passthrough'
                      )
              
              model = Pipeline(steps=[
                ('preprocessor', preprocessor),
                ('classifier',LinearRegression( ))
                  ])

              model.fit(X_train, y_train)

           # Predict on the test data
              y_pred = model.predict(X_test)
              # Evaluate the model
              data =  {'debit_year': [int(month_to_predit[0:4])],
                             'debit_month': [int(month_to_predit[5:6])]}
              x_predit_df = pd.DataFrame(data)
              y_predict_df = model.predict(x_predit_df)
                 # print(y_predict_df)
              predict_value = y_predict_df[0]
              all_parent_labels_df.loc[i, 'predict_value'] = predict_value

      print( all_parent_labels_df       ) 


   # go over range of monthes and check the labels 
   def label_check_per_month (self,month_from_str,month_to_str):

      #month_from_str = "2024-01"
      #month_to_str = "2020-08"
      

      # Parse the string dates into datetime objects
      # The day is set to 1 for consistency
      month_from = datetime.datetime.strptime(month_from_str, "%Y-%m")
      month_to = datetime.datetime.strptime(month_to_str, "%Y-%m")

      # Initialize the current month to the start date
      current_month = month_from
      # Create a list to store the month strings
      all_months = []

      # this is the combile data frame reault 
      combine_df = None 

      # Loop from the start month backward to the end month
      while current_month <= month_to:
          # Append the current month in "YYYY-MM" format to the list
          all_months.append(current_month.strftime("%Y-%m"))
          # Subtract one month
          current_month_str = current_month.strftime("%Y-%m")
          print(current_month_str)
          single_month_df = self.predict_label_for_transaction(current_month_str)
         # print(single_month_df)
          if combine_df is None :
             combine_df =  single_month_df
          else :
             combine_df = pd.concat([combine_df,single_month_df], ignore_index=True)

         # print (combine_df)
          current_month += relativedelta(months=1)
          

      # Print the list of all months
      #print(all_months)

      #result_df = combine_df
      #print(result_df)

      return combine_df
      
##      print(X)
##      print(y)
##      print(y_pred_new)
          
##      #first_40_rows = df.head(40)
##      #remaining_rows = df.iloc[40:]
##      train_data_frame=result_data_frame.head(train_counter)
##      test_data_frame=result_data_frame.iloc[train_counter:]
##
##      X_train = train_data_frame[['debit_year','debit_month']]
##      y_train = train_data_frame[['sum_debit']]
##
##      X_test = test_data_frame[['debit_year','debit_month']]
##      y_test = test_data_frame[['sum_debit']]
##
##      ##print(X_train)
##      ##print(y_train)
##      ##print(X_test)
##      ##print(y_test)
##
##
##      #model = linear_model.LinearRegression() #1718
##      #model = ensemble.GradientBoostingRegressor()   # 935
##      #model = ensemble.GradientBoostingRegressor(n_estimators=10, learning_rate=.1,max_depth=10, min_samples_split= 10,min_samples_leaf = 4,subsample= 0.5, max_features= 'sqrt')
##      #model = linear_model.Ridge()   # 1708
##      #model = linear_model.Lasso()   #1717
##      model = linear_model.ElasticNet()  #1442
##      #model = linear_model.HuberRegressor()  #794
##
##      model.fit(X_train,y_train)
##      y_test_predict = model.predict(X_test)
##
##
##      df_all = pd.DataFrame(y_test, columns=['sum_debit'])
##      df_all.insert(1,"predict_data",y_test_predict,True)
##
##
##
##      mae = mean_absolute_error(y_test, y_test_predict)
##      avg_test = df_all['sum_debit'].mean()
##
##      print ("mae is:", mae)
##      print ("avg_test is:", avg_test)
##      print ("percent_error is :", 100*mae/abs(avg_test))
##
##      X_new = pd.DataFrame({
##          'debit_year': [2025],
##          'debit_month': ['07']
##      })
##      y_new = model.predict(X_new)
##      print(y_new)
##
##      #tune_params_to_gradient_boost (X_train,y_train)
##         
##      plt.plot(df_all["sum_debit"].values , label='test_data')
##      plt.plot(df_all["predict_data"].values , label='predict_data')
##         # plt.plot(df_all["SMA13"].values , label='sma13')
##
##        #  plt.plot(Y_test.values, label='Actual')
##        #  plt.plot(Y_test_predict_df, label='Predicted')
##      plt.legend()
##      plt.show()
##
##      model_list = [ensemble.GradientBoostingRegressor(),
##              linear_model.LinearRegression(),
##              linear_model.Ridge(),
##              linear_model.Lasso(),
##              linear_model.ElasticNet(),
##              linear_model.HuberRegressor()]
##
##      for model in model_list :
##       temp_result_model,error_percent,mae = run_ml_model(model,X_train,X_test,y_train,y_test)   



    
print ('-------------------START------------------')
# initialise json param from the following file
#after this json param in available global variable #param_json is available
iniialise_param_json ('G:\\RON\\PHYTON\\PROGRAMS\\MonyManager\\PARAM\\moneymanagerparam.txt')
#mydbcon =  create_connection('G:\\RON\\SQLITE3\\DB\\mymoney_v2.db')
mydbcon =  create_connection(param_json["db_file_name"])
mydbcon.execute("PRAGMA foreign_keys = 1")
#myres = execute_sql(mydbcon,"select * from r1")

mml =  MyMlearning(None)
##mml.predict_label_for_transaction('2025-06')


#df_result = mml.run_parent_key_prediction_with_GradientBoost('2025_06')
##df_result = mml.run_parent_key_prediction('2025_06')
#mml.predit_parent_label_debit_new('2025_06')
#print(df_result)

##if __name__ == "__main__":
##    app = MainGui()
##    app.mainloop()

#----------------------------START GUI -----------------------------
if __name__ == "__main__":
    app = QueryGui(1)
    app.mainloop()
#----------------------------END GUI -----------------------------






##   -----------------------    TEST PRoCEDURE -----------------------------------

###def proc_load_stage_to_trans():
### intialize the message text
##message_text = ''
##
### run the new records count and add it to the message text

##source_budget_year = '2025'
##destination_budget_year ='2026'
### how many monthes exists for the source year
### the relative value of 12/rel_month will be multiply on the budget records
##realtive_monthes_avilable_in_source = 10
###proc_method = 'no_overwrite'
##
##
##query_text = " select distinct year from budget where year = ? "
##args = [destination_budget_year]
##mq =  MyQuery(mydbcon,query_text,args)
##mq.execute_simple_query()
##
##budg_year_flag =len(mq.query_simple_result)
##
###  if rows from this year doesnt  exists then  append 
##if budg_year_flag == 0:
##
##   query_text = " select ? year,iif (l.level > 1 , substr(t.label,0,length(substr(t.label,0,instr(t.label,'\\')))+instr(substr(t.label,instr(t.label,'\\')+1),'\\')+1),t.label) label, \
##               ''  description ,\
##               sum(debit_amount) *12/? budget_amount \
##               from transactions t  \
##               join labels l  \
##               on t.label= l.label \
##               where   strftime('%Y',debit_date) = ?  \
##               and t.label not like '%כרטיס%' \
##               group by ?,iif (l.level > 1 , substr(t.label,0,length(substr(t.label,0,instr(t.label,'\\')))+instr(substr(t.label,instr(t.label,'\\')+1),'\\')+1),t.label)  "
##
##   args = [destination_budget_year,realtive_monthes_avilable_in_source, source_budget_year,destination_budget_year ]
##   mq =  MyQuery(mydbcon,query_text,args)
##   mq.execute_pandas_query()
##   result_df = mq.query_pandas_result_df
##   print(result_df)
##
##   result_df.to_sql(
##           name='budget', 
##           con=mydbcon, 
##           if_exists='append', 
##           index=False
##       )
##   #  if rows from this year does   exists - DONT   append 
##else:
##    dest_year = mq.query_simple_result[0][0]
##    print( dest_year)
##    new_data = {
##      'Result': ['Year Allready exists']
##            }
##    result_df = pd.DataFrame(new_data)
##
##
##df_exec = result_df



#    def __init__(self, dbConnection,query_text,args):


#result_query,cursor_desc = execute_sql_no_except(mydbcon,query_text)
#for i in result_query:
#  for j in range (0,2):
#    print (i[j])
###df_year = self.get_df_from_query (query_text)
##result_query,cursor_desc = execute_sql_no_except(mydbcon,query_text)
##message_text = "number of new records : " + str(result_query[0][0]) +  "\n"
##
### count current transactions  pre insert 
##query_text = "select count(*),max(trans_id)  from transactions"
##result_query,cursor_desc = execute_sql_no_except(mydbcon,query_text)
##pre_load_count = result_query[0][0]
##pre_load_trans_id = result_query[0][1]
##
### insert the new stage records 
###filename = param_json["procedure_sql_dir"]+"stage_insert_into_trans.txt"
###f = open(filename, "r")
##query_text = " insert into transactions \
##    (src_account_id,trans_date,trans_amount,trans_curr,debit_date,  \
##     debit_amount,trans_desc,balance,remark1,remark2,label,ref_account_id)  \
##    select   \
##    src_account_id,trans_date,trans_amount,trans_curr,debit_date,   \
##     debit_amount,trans_desc,balance,remark1,remark2,NULL,NULL    \
##    from transactions_stage   \
##    where (src_account_id,debit_date,debit_amount,trans_desc)    \
##    in    \
##    (   \
##    select src_account_id,debit_date,debit_amount,trans_desc   \
##    from transactions_stage    \
##    except    \
##    select src_account_id,debit_date,debit_amount,trans_desc   \
##    from transactions   \
##    )   \
##    order by trans_date   "
###print(query_text)
##result_query,cursor_desc = execute_sql_no_except(mydbcon,query_text)
##try :
##    result_query,cursor_desc = execute_sql_no_except(mydbcon,'commit ')
##except :
##    None
##
### find the last trans_id  after insert of the new stage records 
##query_text = "select count(*), max(trans_id) from transactions"
##result_query,cursor_desc = execute_sql_no_except(mydbcon,query_text)
### print(query_text)
##after_load_count = result_query[0][0]
###pre_load_trans_id = result_query[0][1]
##
###print(pre_load_trans_id)
##
##
##message_text = message_text +"number of records loaded : " +str(after_load_count - pre_load_count) +"\n"
### update null labels
###filename = param_json["procedure_sql_dir"]+"trans_update_null_labels.txt"
###f = open(filename, "r")
##query_text = "  update transactions   \
##     set  label = ( select distinct  label  \
##       from transactions tt  \
##       where tt.trans_desc = transactions.trans_desc)  \
##     where label is null  "
##    
##
##result_query,cursor_desc = execute_sql_no_except(mydbcon,query_text)
##try :
##    result_query,cursor_desc = execute_sql_no_except(mydbcon,'commit ')
##except :
##    None
##
### check how many null labels still found         
##result_query,cursor_desc = execute_sql_no_except(mydbcon,query_text)
##query_text = " select count(*) from transactions where label is null "
##result_query,cursor_desc = execute_sql_no_except(mydbcon,query_text)
##message_text = message_text +"number of null labels : " +str(result_query[0][0]) +"\n"
##
### update ref_account_id 
###filename = param_json["procedure_sql_dir"]+"trans_update_ref_account_id.txt"
###f = open(filename, "r")
##query_text = "  update transactions  \
##    set ref_account_id =  \
##    (   \
##    select distinct ref_account_id   \
##    from transactions tt   \
##    where transactions.trans_desc = tt.trans_desc   \
##    )   \
##    where ref_account_id is null   \
##    and trans_desc in   \
##    (    \
##    select distinct trans_desc   \
##    from transactions   \
##    where ref_account_id is not null)   "
##result_query,cursor_desc = execute_sql_no_except(mydbcon,query_text)
##try :
##    result_query,cursor_desc = execute_sql_no_except(mydbcon,'commit ')
##except :
##    None
##
### update null_trans_amount
###filename = param_json["procedure_sql_dir"]+"trans_update_ref_account_id.txt"
###f = open(filename, "r")
##query_text = "  update transactions \
##      set trans_amount = debit_amount \
##      where trans_amount =''   "
##
##result_query,cursor_desc = execute_sql_no_except(mydbcon,query_text)
##
##try :
##    result_query,cursor_desc = execute_sql_no_except(mydbcon,'commit ')
##except :
##    None
##
##messagebox.showinfo(title="load info " , message= message_text)
##params = [pre_load_trans_id]
###args.append(pre_load_trans_id)
###print(pre_load_trans_id,after_load_trans_id)
###result_df = self.get_df_from_query (" select * from transactions where trans_id > ?",args)
###df_exec = df_result
###print(params)
##df_exec = pd.read_sql_query('select * from transactions where trans_id > ? ',mydbcon,params=params)
###print(df_exec)


##   -----------------------    END PRoCEDURE -----------------------------------


#proc_load_stage_to_trans()
  
print ('--------------------END----------------------')
