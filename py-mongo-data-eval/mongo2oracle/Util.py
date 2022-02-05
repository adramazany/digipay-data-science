import datetime

import jdatetime
import numpy as np
import pytz


class Util:
    def str_abbreviation_first_word(self,st,limit,sep='_',start_pos=0):
        while len(st)>limit:
            sep_first_pos=st.find(sep,start_pos)+1
            st=(st[0:start_pos+1]+"_"+st[sep_first_pos:])
            start_pos=start_pos+2
            if start_pos>limit:
                return st[:limit]
        return st
    ##################################################################################
    def mylen(self,obj):
        if isinstance(obj,str):
            return len(obj)
        elif isinstance(obj,int):
            return 12
        elif isinstance(obj,float):
            return 18
        else:
            return len(str(obj))
    ##################################################################################
    def myisnumeric(self,obj):
        if isinstance(obj,int) or isinstance(obj,float) or isinstance(obj,complex):
            return True
        else:
            return False

    ##################################################################################
    def clean_column_name(self,col):
        #column name is number
        if self.myisnumeric(col):
            col='N_%s'%(col)
        #remove non english chars
        col_en = col.encode("ascii", "ignore").decode().replace(" ","_").replace("\\","_") \
            .replace(".","_").replace(")","_").replace("(","_") \
            .replace("_id","id").replace("_class","class")
        if len(str(col_en))==0:
            col='U_%s'%(len(col))
        else:
            col=col_en
        #first char is digit
        if col[0].isdigit() or col[0]=='_':
            col='N_%s'%(col)
        #limit of column name is 31
        if len(col)>31:
            col = self.str_abbreviation_first_word(col,31)
        # return col.upper()
        return col.upper().replace("OPTION","OPTION_")

    ##################################################################################
    def clean_column_names(self,columns):
        cleans_columns=list()
        for col in columns :
            cleans_columns.append(self.clean_column_name(col))
        return cleans_columns

    ##################################################################################
    def clean_column_name_length_isnumeric(self,df):
        cleans_columns=dict()

        # print('df.dtypes=',df.dtypes)
        if len(df)>0 :
            # measurer = np.vectorize(len)
            measurer = np.vectorize(self.mylen)
            df_columns_length = dict(zip(df, measurer(df.values).max(axis=0)))
            # df_apply_str = df.apply(str)
            # df_columns_length = dict(zip(df_apply_str, measurer(df_apply_str.values).max(axis=0)))

            # df_columns_isnumeric=df.apply(lambda s: pandas.to_numeric(s, errors='coerce').notnull().all())
            df_columns_numeric_medians = df.agg(['median','max'])
            # print('df_columns_numeric_medians=',df_columns_numeric_medians)
            # df_columns_isnumeric=dict()
            # for col in df.columns : df_columns_isnumeric[col]=(col in df_columns_numeric_medians)
            # print('df_columns_isnumeric=',df_columns_isnumeric)

            for col in df.columns :
                val={'original':col
                    ,'length':(df_columns_length[col] if df_columns_length[col]<=4000 else 4000)
                    ,'isnumeric':(col in df_columns_numeric_medians and 'median' in df_columns_numeric_medians[col] and  df_columns_numeric_medians[col]['median'] is not np.NAN)
                    ,'isfloat':  (col in df_columns_numeric_medians and 'median' in df_columns_numeric_medians[col] and df_columns_numeric_medians[col]['median'] is not np.NAN
                                  and 'max' in df_columns_numeric_medians[col] and myisnumeric(df_columns_numeric_medians[col]['max']) and (float(df_columns_numeric_medians[col]['max'])%1>0) )
                     }
                # clean_name = col.replace(".","_").replace("_id","id").replace("_class","class")[:31].upper()
                clean_name = self.clean_column_name(col)
                # col = col[:31]
                cleans_columns[clean_name]=val

        # print('cleans_columns:',cleans_columns)
        return cleans_columns

    ##################################################################################
    def add_jalali_dates(self,data,timestamp_column_names,timezone=pytz.timezone('Asia/Tehran')):
        for col in timestamp_column_names :
            if col in data:
                data['jd_'+col] = data[col].apply(lambda x: str(jdatetime.datetime.fromtimestamp(float(x)/1000).astimezone(timezone).strftime('%Y%m%d')) if x and str(x).lower()!='nan' else x )
    ##################################################################################
    def add_gregorian_dates(self,data,timestamp_column_names,timezone=pytz.timezone('Asia/Tehran')):
        for col in timestamp_column_names :
            if col in data:
                # data['gd_'+col] = data[col].apply(lambda x: str(datetime.datetime.fromtimestamp(float(x)/1000).astimezone(timezone).strftime('%Y-%m-%d %H:%M:%S')) if x and str(x).lower()!='nan' else x )
                data['gd_'+col] = data[col].apply(lambda x: str(datetime.datetime.fromtimestamp(float(x)/1000).astimezone(timezone).strftime('%d-%b-%y %I:%M:%S %p')) if x and str(x).lower()!='nan' else x )
    ##################################################################################
    def trun_byte(self,src, byte_limit, encoding='utf-8'):
        if self.myisnumeric(src):
            src=str(src)
        if self.getsizeof(src)<byte_limit:
            return src
        else:
            return src.encode(encoding)[:byte_limit].decode(encoding, 'ignore')
