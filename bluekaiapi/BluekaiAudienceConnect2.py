#Paqueterias de Python 3+ Necesarias
import os
import sys
import urllib
import http.cookiejar
import hashlib 
import hmac
import base64
import json
import random
import http
import requests
import pandas as pd
import time

from urllib.request import urlopen
from urllib.parse import urlparse

headers = {"Accept":"application/json","Content-type":"application/json"}
#--------------------------------------------------------------------------------------------------------------

def InputBuilder(bkuid, bksecretkey,url, method, data):
        print("cual es el metodo?",method)
        stringToSign = method
        parsedUrl = urlparse(url)
        print (parsedUrl)
        stringToSign += parsedUrl.path

        # splitting the query into array of parameters separated by the '&' character
        #print parsedUrl.query
        qP = parsedUrl.query.split('&')
        #print qP

        if len(qP) > 0:
            for  qS in qP:
                qP2 = qS.split('=', 1)
                #print qP2
                if len(qP2) > 1:
                    stringToSign += qP2[1]

        #print stringToSign
        if method == 'POST':
            # data = '{"AND":[{"AND":[{"OR":[{"cat":595020}]}]}]}';
            print("entro al post")
        print("data en conector ",data)
        # print("data en conector ",json.dumps(data) )
        
        if data != None :
            print("entro a agregar data en bksig")
            stringToSign += str(data)
            # stringToSign += json.dumps(data)
        print ("\nString to be Signed:\n" + stringToSign)

        # Encoding for hmac method
        stringToSign = stringToSign.encode("utf-8")
        h = hmac.new(bksecretkey.encode("utf-8"), stringToSign, hashlib.sha256)

        s = base64.standard_b64encode(h.digest())
        print ("\nRaw Method Signature:\n" + s.decode("utf-8") )

        u = urllib.parse.quote(s.decode("utf-8"), safe="")
        print ("\nURL Encoded Method Signature (bksig):\n" + u)

        newUrl = url 
        if url.find('?') == -1 :
            newUrl += '?'
        else:
            newUrl += '&'

        newUrl += 'bkuid=' + bkuid + '&bksig=' + u 
        print("Signed URL: "+newUrl)
        return newUrl

def doRequest(url, method, data):

        # print("data en dorequest ",json.dumps(data))
        try:
            cJ = http.cookiejar.CookieJar()
            request = None
            if method == 'PUT': 
                request = urllib.request.Request(url, data, headers)
                request.get_method = lambda: 'PUT'
            elif  method == 'DELETE' :
                request = urllib.request.Request(url, data, headers)
                request.get_method = lambda: 'DELETE'
            elif method == "POST":
                print("Post Method!")
                # data = urllib.parse.urlencode(data) #I added this
                # data = json.dumps(data)
                # data = data.encode('utf-8')
                # data = str(data)
                # print(type(data))
                # print ("data final2 ",data)
                # data = urllib.parse.quote(data)
                # data = data.decode('utf-8')
                # data = json.dumps(data)
                # data = data.encode('utf-8')
                # data = data.replace("[1,None]", "1")
                data = str(data)
                data = data.replace(" ", "")
                data = data.replace("'", "\"")
                data = data.replace("None", "null")
                # print ("data final ",data)
                # print("url en dorequest ",type(url))
                # print("method en dorequest ",type(method))
                # print("data en dorequest ", type(data))
                request = urllib.request.Request(url, data.encode('utf-8'), headers)
                request.get_method = lambda: 'POST'
                # print("request", request)
                opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cJ))
                u = opener.open(request)
                rawData = u.read()
                
                print ("\nResponse Code: 200")
                # print ("\nAPI Response:\n" + rawData.decode("utf-8") + "\n")
                print("\n------------------------------------------------------> SUCESS!!!\n")
                # return True
                print("rawdata -->", rawData.decode("UTF-8"))
                return rawData.decode("UTF-8")
            elif data != None :
                request = urllib.request.Request(url, data, headers)
            else:
                request = urllib.request.Request(url, None, headers)  
                opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cJ))
                u = opener.open(request)
                rawData = u.read()
                print ("\nResponse Code: 200")
                # with open('data.json', 'w', encoding='utf-8') as f:
                # f.write(rawData.decode('utf-8'))
                # print(rawData) 
        ###elchido
                # print ("\nAPI Response:\n" + rawData.decode("utf-8") + "\n")
                return rawData.decode("UTF-8")

        except urllib.error.HTTPError as e:
            print ("\nHTTP error: %d %s" % (e.code, str(e)) )
            print ("ERROR: ", e.read())
            return "{}"
        except urllib.error.URLError as e:
            print ("Network error: %s" % e.reason.args[1])
            print ("ERROR: ", e.read())
            return "{}"


        
class BluekaiAudienceCall:

    #Creating the method signature
    def __init__(self,url, method, data=None):
        print("metodo !!!! ", method)
        self.url = url
        self.method = method
        self.data = data
    
    def SignInput (self, bkuid, bksecretkey):
        self.signedUrl = InputBuilder(bkuid, bksecretkey,self.url, self.method, self.data)
    

    def ListAudience(self, pid, bkuid, bksecretkey):
        Url = "https://services.bluekai.com/Services/WS/audiences?pid="+pid+"&label=hexagondata"
        signedUrl = InputBuilder(bkuid, bksecretkey, Url, self.method, None)
        
        Request = doRequest(signedUrl, "GET", None)
        
        r1 = json.loads(Request)
        
        return r1
    
    def DetailAudience(self, pid, audienceID, bkuid, bksecretkey):
        Url = "https://services.bluekai.com/Services/WS/audiences/"+audienceID+"/?pid="+pid
        print("url para detalle", Url);
        signedUrl = InputBuilder(bkuid, bksecretkey, Url, self.method, None)
        
        Request = doRequest(signedUrl, "GET", None)
        
        r1 = json.loads(Request)
        
        
        return r1

    def ReachAudience(self, pid, audienceID, bkuid, bksecretkey, data):
       
        Url = "https://services.bluekai.com/Services/WS/SegmentInventory?pid="+pid
        # data = '{"AND":[{"AND":[{"OR":[{"cat":595020}]}]}]}'
        # data = '{"AND":[{"AND":[{"OR":[{"cat":595020,"freq":1},{"cat":630949,"freq":1},{"cat":630950,"freq":1},{"cat":630952,"freq":1},{"cat":630955,"freq":1},{"cat":630957,"freq":1},{"cat":639871,"freq":1},{"cat":1400385,"freq":1},{"cat":1402275,"freq":1},{"cat":1552728,"freq":1}]}]}]}'
        data = str(data)
        data = data.replace(" ", "")
        # data = data.replace("[1,None]", "1")
        data = data.replace("'", "\"")
        data = data.replace("None", "null")
        signedUrl = InputBuilder(bkuid, bksecretkey, Url, "POST", data)
        # data = urllib.parse.urlencode(data)
        
        # data = json.dumps(data)
        # data = data.encode('utf-8')
        print("data en reach\n", data);
        print(type(data));
        Request = doRequest(signedUrl, "POST", data)
        
        r1 = json.loads(Request)
        return r1