import requests
import pandas as pd
from datetime import datetime as dt

import xml.etree.ElementTree as et

url = "http://ec.europa.eu/taxation_customs/vies/services/checkVatService"

#url = "http://ec.europa.eu/taxation_customs"

vat_code = "02087509"
vat_country = "FI"


#payload = "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">\r\n    <SOAP-ENV:Body>\r\n        <checkVat xmlns=\"urn:ec.europa.eu:taxud:vies:services:checkVat:types\">\r\n            <countryCode>FI</countryCode>\r\n            <vatNumber>02087509</vatNumber>\r\n        </checkVat>\r\n    </SOAP-ENV:Body>\r\n</SOAP-ENV:Envelope>"

payload = "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">\r\n    <SOAP-ENV:Body>\r\n        <checkVat xmlns=\"urn:ec.europa.eu:taxud:vies:services:checkVat:types\">\r\n            <countryCode>"+vat_country+"</countryCode>\r\n            <vatNumber>"+vat_code+"</vatNumber>\r\n        </checkVat>\r\n    </SOAP-ENV:Body>\r\n</SOAP-ENV:Envelope>"


headers = {
  'Content-Type': 'text/xml'
}

try:
    
 response = requests.request("POST", url, headers=headers, data=payload)
 
except:
    print("vat service not working")

print(response.text)

response = response.text.replace("ns2:","")

#print(response)


#xtree = et.parse("xyz.xml")
#xroot = xtree.getroot()
 
root = et.ElementTree(et.fromstring(response))


root = root.getroot()

#print(root)



# for child in root:
#     print(child.tag)
#     print(child.attrib)

# for node in root:
#     for item in node:
#         print(item.tag, item.attrib)
#         for child in item:
#             print(child.tag, child.attrib)
                
#             vatNumber = item.find('vatNumber').text
#             valid = item.find('valid').text
            
#             #vatNumber = item.get('vatNumber')
        
#             valid = item.find('valid').text
#             print(f' {vatNumber} status is {valid}')
            
       
    
for node in root.iter('checkVatResponse'):
#      print(node.tag)
#      #print(node.attrib)
        vatNumber = node.find('vatNumber').text
        valid = node.find('valid').text
        name = node.find('name').text
        countryCode = node.find('countryCode').text
        requestDate = node.find('requestDate').text
        address = node.find('address').text
        
        print(f' VAT code  {vatNumber} country "{countryCode}" status is "{valid}"')



df_cols = ["countryCode", "vatNumber", "requestDate", "valid","name","address"]


# Extracting the data
rows = []


if valid == 'true' or valid == 'false'  :
    
    rows.append({"countryCode": countryCode, "vatNumber": vatNumber, 
                  "valid": valid, "name": name, "address": address, "requestDate": requestDate})

    df = pd.DataFrame(rows, columns = df_cols)

    print(df)
else:
    print("No rows retured")





