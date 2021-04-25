#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 31 13:51:06 2020

@author: appspro
"""

from datetime import datetime
from elasticsearch import Elasticsearch
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import pandas as pd
from difflib import SequenceMatcher
from unidecode import unidecode
import numpy as np
import math
#from langdetect import detect
#from lang_trans.arabic import buckwalter
import string
import sys
import argparse
# import translators as ts
# from translate import translator
from bidi.algorithm import get_display
import arabic_reshaper
from sklearn.metrics.pairwise import cosine_similarity 
from validate import validations
from preprocssing import preprocssing
    
class Operations () : 
    
    def __init__(self) : 
        self.es = Elasticsearch( 
                        hosts=['15.185.41.105:9200'],
                        http_auth=('elastic', 'amro_alfares'),
                       )
        
        self.settings_file = pd.read_csv("static/files/settings.csv")


    def generate_query (self, index :str, fileds:dict, size = None, _source =None  , _sort=None  ) -> dict :
        
        # Phonetics lists
        phonetics_must_list = list()
        phonetics_must_not_list = list()
        phonetics_should_list = list()

        # Deterministic lists
        deterministic_must_list = list()
        deterministic_must_not_list = list()
        deterministic_should_list = list()

        query = { 

            "query": {
                "bool": {
                    "must": [],
                    "filter": [],
                    "should": [],
                    "must_not": [] 
                }
            }

        }

        if _source != None : 
            query['_source'] = _source

        if size != None : 
            if size > 0 : 
                query['size'] = size

       
        settings = self.settings_file[self.settings_file['index'] == index]

        for key , value in fileds.items() : 

            if value == '' or value == [] : 
                continue 
            key_info = settings[settings['field']== key]
            # Phonetics Fields
            if key_info.iloc[0,:]["search_type"] == "phonetics" : 
                
                match = {"match":dict() }
                key_dict = { "query": value,"fuzziness": "{}".format(key_info.iloc[0,:]["fuzzi_value"]), "operator": "and" }
                match['match'][key] = key_dict

                if key_info.iloc[0,:]["operations"] == "and" :
                    phonetics_must_list.append(match)
                elif key_info.iloc[0,:]["operations"] == "or" :
                    phonetics_should_list.append(match)
                elif key_info.iloc[0,:]["operations"] == "not" :
                    phonetics_must_not_list.append(match)

            # Deterministic Fields
            elif key_info.iloc[0,:]["search_type"] == "deterministic" : 

                term = {"term":dict() } 
                terms = {"terms":dict() }

                
                # Deterministic Single Value 
                if key_info.iloc[0,:]["value_type"] == "single" :
                    
                    term ["term"][key] ={ 'value':value} 

                    if key_info.iloc[0,:]["operations"] == "and" :
                        deterministic_must_list.append(term)
                    elif key_info.iloc[0,:]["operations"] == "or" :
                        deterministic_should_list.append(term)
                    elif key_info.iloc[0,:]["operations"] == "not" :
                        deterministic_must_not_list.append(term)

                # Deterministic Mulitnale Value 
                elif key_info.iloc[0,:]["value_type"] == "mulitnale" :
                    
                    if type(value) == list and len (value) > 1: 

                        terms ["terms"][key] ={ 'value':value} 
                        if key_info.iloc[0,:]["operations"] == "and" :
                            deterministic_must_list.append(terms)
                        elif key_info.iloc[0,:]["operations"] == "or" :
                            deterministic_should_list.append(terms)
                        elif key_info.iloc[0,:]["operations"] == "not" :
                            deterministic_must_not_list.append(terms)

                    else :

                        term ["term"][key] ={ 'value':value} 
                        if key_info.iloc[0,:]["operations"] == "and" :
                             deterministic_must_list.append(term)
                        elif key_info.iloc[0,:]["operations"] == "or" :
                            deterministic_should_list.append(term)
                        elif key_info.iloc[0,:]["operations"] == "not" :
                            deterministic_must_not_list.append(term)


        if "first_name_en" in fileds.keys() : 
            if "abd" ==  fileds['fileds']["first_name_en"][0:3].lower() : 
                    if fileds['fileds']["first_name_en"][3] == " " : 
                        new_value = "abd"+fileds['fileds']["first_name_en"][4:]
                    else : 
                        new_value = "abd "+fileds['fileds']["first_name_en"][4:]

                    match = {"match":dict() } 
                    key_dict = { "query": new_value,"fuzziness": "AUTO", "operator": "and" }
                    match['match']["first_name_en"] = key_dict
                    phonetics_must_list.append(match)

            elif "abed" ==  fileds['fileds']["first_name_en"][0:4].lower() : 
                    if fileds['fileds']["first_name_en"][4] == " " : 
                        new_value = "abed"+fileds['fileds']["first_name_en"][5:]
                    else : 
                        new_value = "abed "+fileds['fileds']["first_name_en"][5:]

                    match = {"match":dict() }
                    key_dict = { "query": new_value,"fuzziness": "AUTO", "operator": "and" }
                    match['match']["first_name_en"] = key_dict
                    phonetics_must_list.append(match)


        # Phonetics  List 
        query['query']['bool']['must'].append( {"bool":{"should": phonetics_must_list }} ) 
        query['query']['bool']['must'].append( {"bool":{"should": phonetics_should_list }} ) 
        query['query']['bool']['must'].append( {"bool":{"must_not": phonetics_must_not_list }} ) 

        # Deterministic List
        query['query']['bool']['must'].append( {"bool":{"must": deterministic_must_list }} )
        query['query']['bool']['must'].append( {"bool":{"should": deterministic_should_list }} )
        query['query']['bool']['must'].append( {"bool":{"must_not": deterministic_must_not_list }} )

        return query



    def phonetics (self ,field_value : str, search_value : str ,language = 'ar', pre_processing='on') -> float : 

        obj_preprocssing = preprocssing() 
        field_value = field_value.lower().strip()
        search_value = search_value.lower().strip()
        if language == 'ar' :

            if  pre_processing == 'on' : 
                field_value  = obj_preprocssing.remove_diacritics(field_value , language = language)
                field_value = obj_preprocssing.normalize(field_value , language = language )
                search_value  = obj_preprocssing.remove_diacritics(search_value , language = language)
                search_value = obj_preprocssing.normalize(search_value , language = language )

            field_value_unidoce = unidecode(field_value)
            search_value_unicode = unidecode(search_value)
            score = SequenceMatcher(None, field_value_unidoce , search_value_unicode).ratio()
            
        elif language == 'en' :
            score= SequenceMatcher(None, field_value  , search_value ).ratio()

        if score == 1 and len (field_value) != len(search_value) : 
            score = score - 0.10 

        elif score < 0.85  and  score > 0 : 
            score = score - 0.05
        return score


    def cosine_sim_vectors (self , vector_1 , vector_2 ) :
        vector_1 = vector_1.reshape(1,-1)
        vector_2 = vector_2.reshape(1,-1)
        scoure = cosine_similarity(vector_1 , vector_2 )

    def calculate_weight_for_object (self , fileds : dict , result : dict , index :str , pre_processing = 'on' , weight_type='local_weight' ) -> dict :

        obj_data = dict()
        settings = self.settings_file[self.settings_file['index'] == index]
            
        for key , value in result.items() :
            key_info = settings[settings['field']== key]
            
            if key in fileds.keys() : 
                if fileds[key] != '' and fileds[key] != [] :

                    if key_info.iloc[0,:]["search_type"] == "phonetics" :

                        if pre_processing == "on" : 
                            if key_info.iloc[0,:]["pre_processing"] == "off" :
                                pre_processing = "off"

                        language = key_info.iloc[0,:]["language"]
                        similarity_ratio = self.phonetics(field_value = fileds[key] , search_value = value , language = language , pre_processing = pre_processing   ) * 100

                    elif  key_info.iloc[0,:]["search_type"] == "deterministic" : 

                        if key_info.iloc[0,:]["value_type"] == "single" :

                            if value == fileds[key] : 
                                similarity_ratio = 100
                            else : 
                                similarity_ratio = 0

                        elif key_info.iloc[0,:]["value_type"] == "mulitnale" :

                            if value in  fileds[key] : 
                                similarity_ratio = 100
                            else : 
                                similarity_ratio = 0
                else :
                    similarity_ratio = 0 
            else :
                similarity_ratio = 0 

            obj_data[key] = { "value" : value , "similarity_ratio" : similarity_ratio }
        #obj_data['overall_similarity_ratio'] = self.calculate_overall_weight( index = index ,result = obj_data , weight_type= weight_type )
        return obj_data

    def calculate_overall_weight (self, index:str, result:dict, weight_type='local_weight' ) -> float : 
        settings = self.settings_file[self.settings_file['index'] == index]
        total = 0
        count = 0 
        for key , value in result.items() :
            key_info = settings[settings['field']== key]
            count +=1
            total = total + value['similarity_ratio']  *  key_info.iloc[0,:][weight_type] 
        total = total/ count 
        return  total 
 
    def search (self, index:str, fileds:dict, size=None, _source=None, _sort=None, pre_processing='on', init_country='jo', return_query=False  ) -> list : 
        query =  self.generate_query (index=index, fileds=fileds, size=size, _source=_source, _sort=_sort)
        if return_query : 
            return query
        result = self.es.search(index=index , body=query)

        phonetics_result = list()
        for obj in result["hits"]["hits"]:

            result_with_weight = self.calculate_weight_for_object(
                 fileds = fileds ,  
                 result = obj['_source'] , 
                 index= index , 
                 pre_processing = pre_processing ,
                 weight_type='local_weight'
                 )
            result_with_weight["elastic_id"] = obj["_id"] 
            phonetics_result.append(result_with_weight)
        return phonetics_result , 200
                

    def add (self , index : str , _object : dict ) -> str : 
        result = self.es.index( index=index , body=_object )
        res = {'msg':'the process of adding done successfully'}
        return res , 200 


    def update (self,index, party_id ,_object) : 
        query =  self.generate_query ( index = index  ,fileds = {"row_id":party_id} )
        result_search  = result = self.es.search( index= index , body = query )['hits']['hits']

        if result_search == [] : 
            return ({'error': 'Party id is not found.'} , 404)

        elastic_id =result_search[0]['_id']
        result = self.es.update( index=index ,doc_type='_doc', id=elastic_id, body={ "doc":_object } )              
        res = {'msg':'object is updated'}
        return (res , 200) 


    def Delete (self, index ,party_id ):

        query =  self.generate_query ( index = index  ,fileds = {"row_id":party_id} )
        result_search  = result = self.es.search( index= index , body = query )['hits']['hits']

        if result_search == [] : 
            return ({'error': 'Party id is not found.'} , 404)
        elastic_id =result_search[0]['_id']
        result = self.es.delete(
                            index=index,
                            id = elastic_id  
                            )
        res = {'msg':'the process of deleting done successfully'}
        return (res , 200)


    def compare (self ,index : str , object_one :dict , object_two : dict  ,pre_processing = 'on', weight_type='local_weight' ) -> dict : 

        result_with_weight = self.calculate_weight_for_object(
                fileds = object_one ,  
                result = object_two , 
                index= index , 
                pre_processing = pre_processing ,
                weight_type='local_weight'
                )
                             
        return result_with_weight , 200



    def get_log (self , index = "log" , size = 100 ) : 
        query = dict()
        query['query'] = { "match_all": {} } 
        query['size'] = size
        result_list = list()
        result = self.es.search(index=index , body=query)['hits']['hits'] #['_source']

        for res in result : 
            result_list.append(res['_source'])

        return result_list , 200


    def add_to_log (self,headers, status,operation,party_type=None,party_id=None ):

        obj_log = dict() 

        if party_type != None : 
            obj_log['party_type']= party_type
        if party_id != None : 
            obj_log['party_id']= party_id

        obj_log['init_country']= headers['Init-Country']
        obj_log['channel_identifier']= headers['Channel-Identifier']
        obj_log['unique_reference']= headers['Unique-Reference']
        obj_log['operation']= operation
        obj_log['time_stamp']= headers['Time-Stamp']
        obj_log['status']= status
        add_obj_log = self.add(index='log' , _object=obj_log )

        return True 

















    def PhoneticsSearchNameFunc(self ,name , index , language ) : 
        
        if language == 'ar' : 
            result = self.es.search(
                                index=index ,
            
            body={ 
                "query": {


                        "match": {
                            "full_name_ar": {
                                "query": name,
                                "fuzziness": "AUTO",
                                "operator": "and"
                                }
                            }



                        }
            }
            )
            
            data={'arabicname':list() , 'englishname':list() , 'id':list()}
            for res in result["hits"]["hits"]:
                res_ar={
                    "id":res["_id"],
                    "full_name_ar":res["_source"]["full_name_ar"]
                    }
                data["arabicname"].append(res_ar)            
                
            dataResponce=list()
            for arabicname_data in data["arabicname"]:
                
                arabicname = preprocssing().remove_diacritics(arabicname_data["full_name_ar"])
                arabicname = preprocssing().normalize_arabic(arabicname)
                arabicname_unidoce = unidecode(arabicname)
                name_unicode = unidecode(name)
                score = SequenceMatcher(None, arabicname_unidoce , name_unicode).ratio()
                arabicname_data['score']=score
                
                if score > 0.6 : 
                    dataResponce.append(arabicname_data)

            return { 'arabic_data' :  dataResponce} 
        
        elif language =='en' : 
            
            result = self.es.search(
            # indexlogstashtest
            index=index ,
            body={ 
                "query": {
                    "match": {
                    "full_name_en": {
                        "query": name,
                        "fuzziness": "AUTO",
                        "operator": "and"
                        }
                    }   
                }
            }
            )

            data={'englishname':list() , 'id':list()}
            for res in result["hits"]["hits"]:
                res_en={
                    "id":res["_id"],
                    "full_name_en":res["_source"]["full_name_en"]
                    }
                data["englishname"].append(res_en)
        
            dataResponce=list()
            for englishname_data in data['englishname']:
                score= SequenceMatcher(None, englishname_data['full_name_en'].lower(), name.lower()).ratio()
                englishname_data['score']= score
                if score > 0.6 : 
                    dataResponce.append(englishname_data)

            return {'english_data': dataResponce} 
        
        else : 
            return 'Please enter Arabic or English Language'
    
    
    def PhoneticsDeleteFunc (self, index , id ):
        try : 
            result = self.es.delete(
                # indexlogstashtest
                index=index ,
                id = id  
                    )
            return result
        except : 
            return {'error':'Id {} is not found'.format(id)}
    
    def PhoneticsAddFunc (self , index , arabicname ,englishname  ) : 
        result = self.es.index(
            index=index ,
            body={
            "full_name_ar" : arabicname ,
            "full_name_en" : englishname 
            }
                )
        return result

    
    def PhoneticsSearchAddressFunc(self,index ,address ,language ) :
        
        if language == 'ar' :  
            result = self.es.search(
                # indexlogstashtest
                index=index ,
                body={ 
                    "query": {
                            "match": {
                                "address_ar": {
                                    "query": address,
                                    "fuzziness": "AUTO",
                                    "operator": "and"
                                 }
                        }
                    }
                }
            )
            
            data={'arabic_address':list() ,'id':list()}
            for res in result["hits"]["hits"]:
                res_ar={
                    "ID":res["_id"],
                    "arabic_address":res["_source"]["arabicAddress"]
                    }
                data["arabic_address"].append(res_ar)
        
            dataResponce=list()
            for arabic_address_data in data["arabic_address"]:
                arabic_address = preprocssing().remove_diacritics(arabic_address_data["arabic_address"])
                arabic_address = preprocssing().normalize_arabic(arabic_address)
                arabic_address_unidecode = unidecode(arabic_address)
                name_unidecode = unidecode(address)
                score = SequenceMatcher(None, arabic_address_unidecode, name_unidecode).ratio()
                arabic_address_data['score']=score
                if score > 0.6 : 
                    dataResponce.append(arabic_address_data)
                else : 
                    pass
            return { 'Arabic Address data' :  dataResponce}  
        
        
        elif language == 'en' :
            
            result = self.es.search(
                # indexlogstashtest
                index=index ,
                body={ 
                    "query": {
                        "match": {
                        "address_en": {
                            "query": address,
                            "fuzziness": "AUTO",
                            "operator": "and"
                        }
                        }   
                    }
                }
            )
        
            data={'english_address':list() , 'id':list()}
            for res in result["hits"]["hits"]:
                res_en={
                    "id":res["_id"],
                    "english_address":res["_source"]["englishAddress"]
                    }
                data["english_address"].append(res_en)
        
            dataResponce=list()
            for english_address_data in data['english_address']:
                score= SequenceMatcher(None, english_address_data['englishAddress'].lower(), address.lower()).ratio()
                english_address_data['score']= score
                if score > 0.6 : 
                    dataResponce.append(english_address_data)
      
            return {'English Address data': dataResponce} 
            
    def PhoneticsAddAddressFunc(self,index,arabicAddress,englishAddress):
        # field = request.form["field"]
        result = self.es.index(
            index=index ,
            body={
            "arabicAddress" : arabicAddress ,
            "englishAddress" : englishAddress 
            }
                )
        return result
            
    
    def PhoneticsMeargeFunc (self,index,data,target):
            
        i=0
        for ids in data:
            if(ids["id"] != target["id"]):
                self.es.delete(
                # indexlogstashtest
                index=index ,
                id = ids["id"]  
                    )
                i+=1  
        return i
    
    def PhoneticsAllFunc (self):
        result = self.es.search(
        index="indexlogstashtest",
        body={
            "query": {
                "match_all": {}
                }
            }
        )
        return result

    def PhonetiMultiIndexMultiFields(self , response):
        search_arr = list()
        for data in response['data'] : 
            # req_head
            search_arr.append({"index":data['index']})
            # req_body

            search_arr.append({"query":{"match":{data['field']:{"query":data['search']}}}})
         
        request = '%s \n'.join(search_arr)
        # for each in search_arr:
        #     request += '%s \n' %json.dumps(each)
        return  self.es.msearch(body = request)
         
    
        


    
    
    
    