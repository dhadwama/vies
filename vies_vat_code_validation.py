# Automation name : Viivi
# Desciption : Verify the validity of a VAT number issued by any Member State from VIES https://ec.europa.eu/taxation_customs/vies/#/vat-validation
# 

import requests
import pandas as pd
import numpy as np
import os
from datetime import datetime as dt
import xml.etree.ElementTree as et
import sys
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import io
# importing module
import time

# Read env file
load_dotenv(".env")


# Open and read the SQL file as a single buffer
fd = open('vies_viivi/vat_number.sql', 'r')
sql = fd.read()
#print(sqlFile)
fd.close()

# Read input parameters

# StartPeriod = sys.argv[1]
# EndPeriod = sys.argv[2]



SF_ACCOUNT    = os.getenv('SF_ACCOUNT')
SF_USER       = os.getenv('SF_USER')
SF_WAREHOUSE  = os.getenv('SF_WAREHOUSE')
SF_DATABASE  = os.getenv('SF_DATABASE')
SF_ROLE     = os.getenv('SF_ROLE')
SF_SCHEMA     = os.getenv('SF_SCHEMA')
SF_PASSWORD   = os.getenv('SF_PASSWORD')


#fire up an instance of a snowflake connection
connection = snowflake.connector.connect (
account  = SF_ACCOUNT,
user     = SF_USER,
password = SF_PASSWORD,
database = SF_DATABASE,
warehouse = SF_WAREHOUSE,
schema = SF_SCHEMA,
role = SF_ROLE
)
con = connection.cursor()

##### GLobal Environment variables #######

df_log_final = pd.DataFrame() # Create empty dataframe to store file Start and End time.


df_error_log_final = pd.DataFrame() # Store Error retured from VIES site 

# Create test error data
#error_test_data = dict(VATNum='08599126', VATCountry='FI')
#df_error_log_final = pd.DataFrame(error_test_data,index=[0])


df_log = pd.DataFrame(columns = ['vat_code','vat_country']) # 

df_final = pd.DataFrame() 

            

################ Get Validation from VIES API ##############################

def vies_vat(df):

    global df_log_final 
    global df_error_log_final
    global df_final

    df1 = df
    temp_count = 0
    
   

    #df1 = df1.query("VATNum  in ('117037668','08599126','106-00-00-553','1060000553') ")

    
    v_country = list(df1['VATCountry'].unique()) # Read unique values from VATCountry field   



    for id in v_country:  # Loop each for each country

        df = df1[df1['VATCountry'].isin([id])] 
            
        df.reset_index(inplace=True)

        print(f'Total input rows :{id} { df.shape[0]}')

                
        #for index, row in df.iterrows():
            #row  = row.to_frame() # convert into Frame
                            
            #row = row.T # Transpose DF
            #row = row[["VATCountry","VATNum"]]
            #vat_code = row["VATNum"].values[0]
            #vat_country = row["VATCountry"].values[0]
        
        for row in df.itertuples(index=True, name='Pandas'):

        #for i in range(len(df)):    
            

            #row = df.loc[i]

            #vat_code = row["VATNum]
            #vat_country = row["VATCountry"]
            
            vat_code = ''
            vat_country = ''
            vat_code = row.VATNum
            vat_country = row.VATCountry
             
            
            df_log.loc[0] = [vat_code,vat_country]


            ############################## VIES API #############################################
            response = ''
            root = ''
            url = "http://ec.europa.eu/taxation_customs/vies/services/checkVatService"

            #url = "http://ec.europa.eu/taxation_customs"

            #vat_code = "0100040"
            #vat_country = "IN"


            #payload = "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">\r\n    <SOAP-ENV:Body>\r\n        <checkVat xmlns=\"urn:ec.europa.eu:taxud:vies:services:checkVat:types\">\r\n            <countryCode>FI</countryCode>\r\n            <vatNumber>02087509</vatNumber>\r\n        </checkVat>\r\n    </SOAP-ENV:Body>\r\n</SOAP-ENV:Envelope>"

            payload = "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">\r\n    <SOAP-ENV:Body>\r\n        <checkVat xmlns=\"urn:ec.europa.eu:taxud:vies:services:checkVat:types\">\r\n            <countryCode>"+vat_country+"</countryCode>\r\n            <vatNumber>"+vat_code+"</vatNumber>\r\n        </checkVat>\r\n    </SOAP-ENV:Body>\r\n</SOAP-ENV:Envelope>"

            #print(payload)

            headers = {
            'Content-Type': 'text/xml'
            }

            try:
                
                response = requests.request("POST", url, headers=headers, data=payload)
            
            except Exception as e:
                print(f'ERROR response from VIES Service {e}')     

            #print(f'print response.text {response.text}')

            try:
                    # Check if output has error
                    if 'Fault' in response.text:
                        response = response.text.replace("env:","")
                        
                        root = et.ElementTree(et.fromstring(response))


                        root = root.getroot()
                        
                        for node in root.iter('Fault'):
                    #      print(node.tag)
                    #      #print(node.attrib)
                            Faultcode = node.find('faultcode').text
                            Faultstring = node.find('faultstring').text
                            vatNumber = vat_code
                            #print(f' Fault code :  {Faultcode} and error :{Faultstring} ')
                            
                            df_error_cols = [ "VATNum", "VATCountry","address","valid"]
                            
                            # Extracting the data
                            rows = []

                            rows.append({"address": Faultcode, "VATNum": vat_code,"VATCountry":vat_country, "valid": Faultstring})

                            df_error_log = pd.DataFrame(rows, columns = df_error_cols)

                            #print(df_error_log)
                            
                            df_error_log_final = pd.concat([df_error_log_final, df_error_log])

                    elif  'checkVatResponse' in response.text:

                        response = response.text.replace("ns2:","")

                        #xtree = et.parse("xyz.xml")
                        #xroot = xtree.getroot()
                    
                        root = et.ElementTree(et.fromstring(response))


                        root = root.getroot()

                        #print(f'print root {root}')

                    
                        
                        for node in root.iter('checkVatResponse'):
                        #      print(node.tag)
                        #      #print(node.attrib)
                                vatNumber = node.find('vatNumber').text
                                valid = node.find('valid').text
                                name = node.find('name').text
                                countryCode = node.find('countryCode').text
                                requestDate = node.find('requestDate').text
                                address = node.find('address').text
                                
                                #print(f' VAT code  {vatNumber} country "{countryCode}" status is "{valid}"')



                        df_cols = ["countryCode", "vatNumber", "requestDate", "valid","name","address"]


                        # Extracting the data
                        rows = []

                        try:
                            if valid == 'true' or valid == 'false'  :
                                
                                rows.append({"countryCode": countryCode, "vatNumber": vatNumber, 
                                            "valid": valid, "name": name, "address": address, "requestDate": requestDate})

                                df_vat = pd.DataFrame(rows, columns = df_cols)
                                
                                #df_vat['requestDate'] = dt.now().strftime("%Y-%m-%d")
                                #print(f'Request data : {df_vat["requestDate"]}')
                                
                                #print(df_vat)
                            else:
                                print("No rows retured")
                            
                            
                           
                            #df_log['vat_input_match'] = np.where(df_log['vat_code'] == df_vat['vatNumber'], 'True', 'False') 

                            
                            df_final = pd.concat([df_final, df_vat])
                            #print(df_final)
                        except Exception as e:
                            print(f'Your error message {e}') 
                        
                                                
                        df_vat = pd.DataFrame()

                        rows = []

                        if df_log.empty:
                            #logging.info(f'Log data is empty')
                            print("df_log No rows retured")       
                        else:
                            df_log_final = pd.concat([df_log_final,df_log],ignore_index=True) 
                
            except Exception as e:
                    print(f'ERROR from response.text VIES Service {e}')     


    #print(df_final.head(5))
        temp_count = df.shape[0] + temp_count
        print(f' {id} difference in output row count : { df_final.shape[0]-temp_count}')

    
    return df_final,df_error_log_final
        
        
################ Main function ##############################


def main():



##### Run vat_number.sql to read VAT number data from FIDO database
    try:
        con.execute(sql) 
        df1 = con.fetch_pandas_all()

    except Exception as e:  
        raise e
 
#### Filter Data for EU countries 
    df1 = df1.query("VATCountry in ('AT','BE','BG','CY','CZ','DK','EE','FI','FR','DE','GR','HU','HR','IT','IE','LV','LT','LU','MT','NL','PL','PT','RO','ES','SK','SI','SE') ")
    #df1 = df1.query("VATCountry in ('EE','PL') ")

    if df1.shape[0] != 0:
        #### Snowflake table name
        table_name = "DWD_RPA_VIES_VAT_NUMBER_VALIDATION"
        column = '"period"'
        
        #Period = '202306'
        
        # conver to string and read Period number to delete rows viivi table. 
        
        period = df1['Period'].iloc[0]
        period = period.astype(str)
        print(f' Period : {period}')
        
        
        ##### Call  vies_vat package to call VIES API
         
        df_final,df_error_log_final = vies_vat(df1)

        #print(df_final)
        #print(df_error_log_final)
        #df_final.drop_duplicates(inplace=True)

        if df_error_log_final.empty:
            print("No error rows retured from VIES")       
        else:
            #print(df_error_log_final)
            df_error_log_final['attempt'] = '1'
            df1 = df_error_log_final
            df_error_log_final = df_error_log_final[0:0]
            print("Waiting for 1 hour to rerun VAT number with error from VIES service")      
            time.sleep(3600) #Start again after 10 minutes

            #time.sleep(6) #Start again after 6 seconds
            
            df_final,df_error_log_final = vies_vat(df1[['VATNum','VATCountry']])
            
            if df_error_log_final.empty:
                print("No error rows retured from VIES")       
            else:

                #### Save Errors into Log_vies_error_log.csv file
                     
                df_error_log_final.to_csv("Log_"+"vies_error_log"+'.csv', encoding='utf-8')

                ### Add vat numbers with errors into final df_final dataframe 
        
        
        if not df_error_log_final.empty:
            
            df_error_log_final = df_error_log_final[df_error_log_final['attempt'] != '1'] 
            df_error_log_final.columns = df_error_log_final.columns.str.replace("VATNum", "vatNumber")
            df_error_log_final.columns = df_error_log_final.columns.str.replace("VATCountry", "countryCode")
            df_error_log_final = df_error_log_final.drop(['attempt'], axis=1)
            df_final = pd.concat([df_final,df_error_log_final],ignore_index=True) 
            
        ###### Save log files to store original data and input data
        

        if df_log_final.empty:
            print("No rows retured")       
        else:
            df_log_final.to_csv("Log_"+"vat_vies"+'.csv', encoding='utf-8')  
 
        
        if df_final.empty:
            print("No rows retured")       
        else:
            
            df_final.to_csv("Log_"+"vat_vies_data"+'.csv', encoding='utf-8')  
            df_final['period'] = period
        #print(column)
        
        try:
            con.execute(    
            "DELETE FROM " + table_name + " WHERE " + column +  "= " + period    ) 
            con.execute("commit")
            success, nchunks, nrows, _ = write_pandas(connection, df_final, table_name)
            
        
        except Exception as e:  
            print(f'ERROR : Final dataframe is empty or write_pandas error {e}') 
            #raise e
        finally:  
            con.close() 



    else :
        print("No rows retured")

    connection.close()


if __name__ == '__main__':
    
    main()



