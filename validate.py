
import pandas as pd 


class validations ():

    def __init__(self) : 

        self.settings_file = pd.read_csv("static/files/settings.csv")
        self.errors = list()


    # Headers Content four functions ( init_country , channel_identifier , unique_reference , time_stamp )

    def validate_init_country (self, value ) :
        return value 

    def validate_channel_identifier(self, value ) :
        return value 

    def validate_unique_reference(self, value ) :
        return value 

    def validate_time_stamp (self, value ) :
        return value 


    # Body 

    def validate_party_id (self , value ) : 
        pass 

    def validate_index(self, value) :

        indexes = list (self.settings_file['index'].unique())

        if type(value) != str : 
            self.errors.append({"index":"the data type  must a string"})
            return value
        if value in indexes : 
            return value
        else : 
            self.errors.append({"index":"this index is not found ({}) Please select index form {}".format(value , indexes )})
            return value


    def validate__source (self,value) :

        fields = list (self.settings_file['field'].unique())

        if value == [] or value == None  :
            return None 

        if type (value) != list : 
            self.errors.append({"_source":"the data type  must a list"})
            return value

        fileds_is_not_found = list()
        for field  in  value : 
            if  field not in fields :
                fileds_is_not_found.append("{} field is not found".format(field))
        if fileds_is_not_found != [] :
            self.errors.append({"_source": fileds_is_not_found })
            return value
        return value


    def validate_pre_processing (self,value) :
        value = value.lower().strip() 
        if  value in ["on","off"] :
            return value
        else : 
            self.errors.append({"pre_processing":"Option '{}' is not found, please select on or off options".format(value)})
            return value

    def validate_size (self, value) :
        if type(value) != int : 
            self.errors.append({"size":"The value must be of the Integer type"})
        if value <= 0 :
            self.errors.append({"size":"size value must be greater than zero"})
        return value 

    def validate__sort (self, value) :

        if value == None : 
            return None 
        elif type(value) != bool : 
            self.errors.append({"_sort":"_sort value must boolean type"})
        return value

    def validate_input_object(self , index , _object ) : 
        for field , value in _object.items() :
            self.validate_field (index = index , field = field , value = value)
        return _object


    def validate_field (self, index ,field, value ) :

        field_settings = self.settings_file[(self.settings_file['index']==index)&(self.settings_file['field']==field)]

        if len (field_settings)  == 0   :
            self.errors.append({"object":"{} field is not found in index {}.".format(field , index )})
            return value

        elif len (field_settings) > 1 : 
            self.errors.append({"object":"{} field is not found in index {}.".format(field , index )})
            return value

        elif type(value).__name__ != field_settings.iloc[0,:]['data_type'].lower().strip() : 
            self.errors.append({"object":"data type {} field must {} type.".format(field , field_settings.iloc[0,:]['data_type'] )})
            return value

        return value