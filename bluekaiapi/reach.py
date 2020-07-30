import os
import sys
from urllib.request import urlopen
from urllib.parse import urlparse
import http.cookiejar
import hashlib 
import hmac
import base64
import json
import random
import urllib

headers = {"Accept":"application/json","Content-type":"application/json","User_Agent":"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.6; en-US; rv:1.9.1) Gecko/20090624 Firefox/3.5"}

# 1. Enter your BlueKai developer keys

bkuid = '72c487d4f0318706ce96a0ebe47b906342cb0a2e3610fe182746602c3be6d016' #Web Service User Key
bksecretkey = '90388366ed94cfaea8ee163e0c64200a587974ce7492ec827b33e84ba644bad0' #Web Service Private Key

# 2.  Specify the service endpoint
#    - For GET (List) requests, add the desired sort and filter options in the query string
#    - For GET (Read), PUT or DELETE requests, append the item ID to the Url path
#     * NOTE: For the Campaign, Order, and Pixel URL APIs, insert the item ID in the query string instead of the Url path

Url = 'https://services.bluekai.com/Services/WS/SegmentInventory?pid=3740'

# 3. For POST and PUT requests, uncomment the "data" variable and enter the JSON body
#data = ''

#Creating the method signature
def signatureInputBuilder(url, method, data):
    stringToSign = method
    parsedUrl = urlparse(url)
    print ("url parse", parsedUrl)
    stringToSign += parsedUrl.path
    
    # splitting the query into array of parameters separated by the '&' character
    print (parsedUrl.query)
    qP = parsedUrl.query.split('&')
    print ("qp ", qP)

    if len(qP) > 0:
        for  qS in qP:
            qP2 = qS.split('=', 1)
            #print qP2
            if len(qP2) > 1:
                stringToSign += qP2[1]
    
    print ("string to sign ", stringToSign)
    if data != None :
        stringToSign += data 
    print ("\nString to be Signed:\n" + stringToSign)
        # Encoding for hmac method
    stringToSign = stringToSign.encode("utf-8")
    h = hmac.new(bksecretkey.encode("utf-8"), stringToSign, hashlib.sha256)

    s = base64.standard_b64encode(h.digest())
    print ("\nRaw Method Signature:\n" + s.decode("utf-8") )

    u = urllib.parse.quote(s.decode("utf-8"))
    print ("\nURL Encoded Method Signature (bksig):\n" + u)

    newUrl = url 
    if url.find('?') == -1 :
        newUrl += '?'
    else:
        newUrl += '&'
        
    newUrl += 'bkuid=' + bkuid + '&bksig=' + u 
    print("new url ", newUrl)
    return newUrl

#Generating  the method request 
def doRequest(url, method, data):
    try:
        cJ = http.cookiejar.CookieJar()
        request = None
        if method == 'PUT': 
            request = urllib.request.Request(url, data, headers)
            request.get_method = lambda: 'PUT'
        elif  method == 'DELETE' :
            request = urllib.request.Request(url, data, headers)
            request.get_method = lambda: 'DELETE'
        elif data != None :
            request = urllib.request.Request(url, data, headers)
        else:
            request = urllib.request.Request(url, None, headers)  
            opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cJ))
            u = urlopen(request)
            rawData = u.read()
            print ("\nResponse Code: 200")
            with open('data.json', 'w', encoding='utf-8') as f:
                f.write(rawData.decode('utf-8'))
            print(rawData) 
            return rawData

    except urllib.error.HTTPError as e:
        print ("\nHTTP error: %d %s" % (e.code, str(e))) 
        print ("ERROR: ", e.read())
        return None
    except urllib.error.HTTPError as e:
        print ("Network error: %s" % e.reason.args[1])
        print ("ERROR: ", e.read())
        return None

#4. Specify the API request method 
def main(argv=None):
    
    # Select the API Method by uncommenting the newUrl reference variable and doRequest() method
    
    # GET
    newUrl = signatureInputBuilder(Url, 'GET', None)
    doRequest(newUrl, 'GET', None)
    # print(newUrl)

    # POST
    #newUrl = signatureInputBuilder(Url, 'POST', data)
    #doRequest(newUrl, 'POST', data)
    
    # PUT
    #newUrl = signatureInputBuilder(Url, 'PUT', data)
    #doRequest(newUrl, 'PUT', data)
    
    #DELETE
    #newUrl = signatureInputBuilder(Url, 'DELETE', None)
    #doRequest(newUrl, 'DELETE', None)
    
    print ("API Call: \n" + newUrl)

if __name__ == "__main__":
   main()