
import pandas as pd 
import re

class preprocssing () : 
    
    def __init__ (self) : 
        
        self.arabic_diacritics = re.compile("""
                                 ّ    | # Tashdid
                                 َ    | # Fatha
                                 ً    | # Tanwin Fath
                                 ُ    | # Damma
                                 ٌ    | # Tanwin Damm
                                 ِ    | # Kasra
                                 ٍ    | # Tanwin Kasr
                                 ْ    | # Sukun
                                 ـ     # Tatwil/Kashida
                             """, re.VERBOSE)
                      
                         
    def remove_diacritics(self,text , language = "en"):
        text = re.sub(self.arabic_diacritics, '', text)
        return text
    
    
    def normalize (self,text , language = "en" ):
        normalize_file = pd.read_csv("static/files/pre_processing.csv")
        normalize_file = normalize_file[normalize_file['language']==language]
        for index , row in normalize_file.iterrows() :
            text = re.sub(row['starting_with'], row['ending_with'], text)
        return text.lower().strip()
