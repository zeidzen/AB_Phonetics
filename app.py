import argparse
import os
# flask imports 
from flask import Flask, request, jsonify, make_response 
#from flask_sqlalchemy import SQLAlchemy env
from flask_swagger_ui import get_swaggerui_blueprint
import uuid # for public id 
from  werkzeug.security import generate_password_hash, check_password_hash 
# imports for PyJWT authentication 
#import jwt 
from datetime import datetime, timedelta 
from functools import wraps 
from phonetics import Operations , validations 
import pandas as pd
#from routes import request_api


# creates Flask object 
app = Flask(__name__) 

search_desc = "The endpoint searches all tables in elastic Search, identify similar objects and returns the results (objects) with the percentage of similarity for all fields and each field separately."


### swagger specific ###
SWAGGER_URL = '/swagger'
API_URL = '/static/swagger.json'
SWAGGERUI_BLUEPRINT = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': "Seans-Python-Flask-REST-Boilerplate"
    }
)
app.register_blueprint(SWAGGERUI_BLUEPRINT, url_prefix=SWAGGER_URL)
### end swagger specific ###


# configuration 
# NEVER HARDCODE YOUR CONFIGURATION IN YOUR CODE 
# INSTEAD CREATE A .env FILE AND STORE IN IT 
app.config['SECRET_KEY'] = 'your secret key'

# database name 
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///Database.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True

# enable debugging mode
app.config["DEBUG"] = True

# Upload folder
UPLOAD_FOLDER = 'static/files'
app.config['UPLOAD_FOLDER'] =  UPLOAD_FOLDER

ROOT_PATH = os.path.dirname(os.path.abspath(__file__))

# creates SQLALCHEMY object 
#db = SQLAlchemy(app) 
   

#app.register_blueprint(request_api.get_blueprint())

# by default we connect to localhost:9200


search_desc = "The endpoint searches all tables in elastic Search, identify similar objects and returns the results (objects) with the percentage of similarity for all fields and each field separately."


@app.route('/phonotics/search/', methods=['POST'])
def Search():  

    required_parameters_keys = ["party_type","_source","pre_processing","size","_sort","organization","search_in_country"] 
    required_keys = ["parameters" , "object" ] 
    required_headers_keys = ['Init-Country','Channel-Identifier','Unique-Reference','Time-Stamp']

    # Get Data and Validate Input request
    data = request.get_json()
    headers = request.headers

    ### Validate Input request  
    for key in required_keys : 
        if key not in data.keys() : 
            return {"Input":"{} key is requierd.".format(key)}

    for key in required_parameters_keys : 
        if key not in data['parameters'].keys() : 
            return {"parameters":"{} key is requierd.".format(key)}

    for key in required_headers_keys : 
        if key not in headers.keys() : 
            return {"headers":"{} key is requierd.".format(key)}

    ### Headers 
    init_country = headers['Init-Country']

    ### Body 
    index = data['parameters']['party_type']
    _source = data['parameters']['_source']
    pre_processing=data['parameters']['pre_processing']
    size = data['parameters']['size']
    _sort = data['parameters']['_sort']
    party_id_not_in = data['parameters']['party_id_not_in']
    organization = data['parameters']['organization']
    party_type = data['parameters']['party_type']
    search_in_country = data['parameters']['search_in_country']
    fileds = data['object']


    # Validate Data 
    obj_validations  = validations()

    ### Headers
    init_country = obj_validations.validate_init_country(init_country)

    ### Body 
    index = obj_validations.validate_index(index)
    _source =obj_validations.validate__source(_source)
    pre_processing = obj_validations.validate_pre_processing(pre_processing)
    size = obj_validations.validate_size(size)
    _sort = obj_validations.validate__sort(_sort)
    fileds = obj_validations.validate_input_object(index =index ,_object = fileds)


    # Ceack errors
    if len (obj_validations.errors) > 0 : 
        return  make_response(jsonify({"errors":obj_validations.errors}), 400) 

    # Get Result
    obj_Operations = Operations()
    result,status = obj_Operations.search(index=index, fileds=fileds, size=size, _source=_source, _sort=_sort, pre_processing=pre_processing, init_country=init_country)

    # Add to log 
    add_obj_log = obj_Operations.add_to_log(headers=headers, status=status, operation="search" , party_type=party_type, party_id=None)
    return make_response(jsonify(result),status)



@app.route('/phonotics/add/', methods=['POST'])
def Add():

    required_parameters_keys = ["party_type","party_id","organization"] 
    required_keys = ["parameters" , "object" ] 
    required_headers_keys = ['Init-Country','Channel-Identifier','Unique-Reference','Time-Stamp']

    # Get Data
    data = request.get_json()
    headers = request.headers
    
    # Validate keys : 
    for key in required_keys : 
        if key not in data.keys() :
            res =  {"errors":obj_validations.errors}
            return make_response(jsonify(res), 400) 

    for key in required_parameters_keys : 
        if key not in data['parameters'].keys() : 
            res =  {"parameters":"{} key is requierd.".format(key)}
            return make_response(jsonify(res), 400) 

    for key in required_headers_keys : 
        if key not in headers.keys() : 
            return {"headers":"{} key is requierd.".format(key)}

    # Get Data 

    ### Headers 
    init_country = headers['Init-Country']
    channel_identifier = headers['Channel-Identifier']
    unique_reference = headers['Unique-Reference']
    time_stamp = headers['Time-Stamp']
    ##Body
    index = data['parameters']['party_type'].lower().strip()
    party_id = data['parameters']['party_id']
    organization = data['parameters']['organization']
    _object=data['object']

    # Validate Data 
    obj_validations  = validations()
    index  = obj_validations.validate_index(index)
    _object = obj_validations.validate_input_object( index , _object )

    if len (obj_validations.errors) > 0 : 
        return  make_response(jsonify({"errors":obj_validations.errors}), 400) 
   
    # Insert row in DataBase 
    obj_Operations = Operations()
    result , status = obj_Operations.add(index , _object )
    result['party_id'] = party_id 
    result['init_country'] = init_country 
    result['channel_identifier'] = channel_identifier 
    result['unique_reference'] = unique_reference 
    result['time_stamp'] = time_stamp 

    # Add to log 
    add_obj_log = obj_Operations.add_to_log(headers=headers, status=status, operation="Add" , party_type=index , party_id=None)

    return make_response(jsonify(result), status)


@app.route('/phonotics/update/<int:party_id>', methods=['PUT'])
def Update(party_id):

    required_parameters_keys = ["party_type","organization"] 
    required_keys = ["parameters" , "object" ] 
    required_headers_keys = ['Init-Country','Channel-Identifier','Unique-Reference','Time-Stamp']

    # Get Data
    data = request.get_json()
    headers = request.headers

    # Validate keys : 
    for key in required_keys : 
        if key not in data.keys() :
            res =  {"errors":obj_validations.errors}
            return make_response(jsonify(res), 400) 


    for key in required_parameters_keys : 
        if key not in data['parameters'].keys() : 
            res =  {"parameters":"{} key is requierd.".format(key)}
            return make_response(jsonify(res), 400) 

    for key in required_headers_keys : 
        if key not in headers.keys() : 
            return {"headers":"{} key is requierd.".format(key)}

    # Get Data 
    ## Headers 
    init_country = headers['Init-Country']
    channel_identifier = headers['Channel-Identifier']
    unique_reference = headers['Unique-Reference']
    time_stamp = headers['Time-Stamp']

    ## Body
    index = data['parameters']['party_type'].lower().strip()
    _object=data['object']

    # Validate Data 
    obj_validations  = validations()
    index  = obj_validations.validate_index(index)
    _object = obj_validations.validate_input_object( index , _object )

    if len (obj_validations.errors) > 0 : 
        return  make_response(jsonify({"errors":obj_validations.errors}), 400) 

    obj_Operations = Operations()
    result , status = obj_Operations.update(index = index , party_id = party_id, _object = _object)
    result['party_id'] = party_id 
    result['init_country'] = init_country 
    result['channel_identifier'] = channel_identifier 
    result['unique_reference'] = unique_reference 
    result['time_stamp'] = time_stamp 

    # Add to log 
    add_obj_log = obj_Operations.add_to_log(headers=headers, status=status, operation="update" , party_type=index , party_id=None)
    return make_response(jsonify(result),status)


@app.route('/phonotics/delete/<int:party_id>', methods=['DELETE'])
def Delete(party_id):
    required_parameters_keys = ["party_type"] 
    required_keys = ["parameters"] 
    required_headers_keys = ['Init-Country','Channel-Identifier','Unique-Reference','Time-Stamp']

    # Get Data
    data = request.get_json()
    headers = request.headers

    # Validate keys : 
    for key in required_keys : 
        if key not in data.keys() :
            res =  {"errors":obj_validations.errors}
            return make_response(jsonify(res), 400) 

    for key in required_parameters_keys : 
        if key not in data['parameters'].keys() : 
            res =  {"parameters":"{} key is requierd.".format(key)}
            return make_response(jsonify(res), 400) 

    for key in required_headers_keys : 
        if key not in headers.keys() : 
            return {"headers":"{} key is requierd.".format(key)}


    ## Headers
    init_country = headers['Init-Country']
    channel_identifier = headers['Channel-Identifier']
    unique_reference = headers['Unique-Reference']
    time_stamp = headers['Time-Stamp']

    ## Body
    index = data['parameters']['party_type'].lower().strip()

    # Validate Data 
    obj_validations  = validations()
    index  = obj_validations.validate_index(index)

    if len (obj_validations.errors) > 0 : 
        return  make_response(jsonify({"errors":obj_validations.errors}), 400) 

    # Delete object
    obj_Operations = Operations()
    result , status = obj_Operations.Delete(index=index , party_id=party_id )
    result['party_id'] = party_id 
    result['init_country'] = init_country 
    result['channel_identifier'] = channel_identifier 
    result['unique_reference'] = unique_reference 
    result['time_stamp'] = time_stamp 

    # Add to log 
    add_obj_log = obj_Operations.add_to_log(headers=headers, status=status, operation="delete" , party_type=index , party_id=None)
    return make_response(jsonify(result), status )


@app.route('/phonotics/changes_file/<party_type>', methods=['POST'])
def ChangesFile (party_type) : 
    # get the uploaded file
    files = request.files['file']
    headers = request.headers

    files.save(os.path.join(ROOT_PATH,files.filename))
    data = pd.read_csv(os.path.join(ROOT_PATH,files.filename))

    # Create Operations 
    for index , row in data.iterrows() :
        obj_Operations = Operations()
        obj_validations  = validations()

        if row['operation'].lower().strip() == "add" : 

            # Get Data 
            data_dict   = dict(row)
            del data_dict['operation']
            del data_dict['index']
            _object = dict()

            for key  in data_dict.keys() :
                if str(data_dict[key]) != 'nan' : 
                    _object[key] = data_dict[key]

            index = row['index'].lower().strip()
            

            # Validate Data 
            index  = obj_validations.validate_index(index)
            _object = obj_validations.validate_input_object( index , _object )
            

            # Insert object in DataBase 
            result , status = obj_Operations.add(index , _object )
            continue

        elif row['operation'].lower().strip() == "update" : 
            # Get Data 
            index = row['index'].lower().strip()
            party_id = row['row_id']

            data_dict   = dict(row)
            _object = dict()
            del data_dict['operation']
            del data_dict['index']
            del data_dict['row_id']

            for key  in data_dict.keys() :
                if str(data_dict[key]) != 'nan' : 
                    _object[key] = data_dict[key]
            
            # Validate Data 
            index  = obj_validations.validate_index(index)
            _object = obj_validations.validate_input_object( index , _object )

            # Update object in DataBase 
            result , status = obj_Operations.update(index = index , party_id = party_id, _object = _object)
            continue

        elif row['operation'].lower().strip() == "delete" : 
            # Get Data 
            index = row['index'].lower().strip()
            party_id = row['row_id']
            # Validate Data 
            index  = obj_validations.validate_index(index)
            # Delete object
            result , status = obj_Operations.Delete(index=index , party_id=party_id )
            continue
        else :
            pass 

    # Add to log 
    add_obj_log = obj_Operations.add_to_log(headers=headers, status=status, operation="changes_file" , party_type=index , party_id=None)
     
    result = {"msg":"Complete"}
    return make_response(jsonify(result), 200 )





@app.route('/phonotics/compare/', methods=['POST'])
def Compare() :

    required_parameters_keys = ["party_type","pre_processing","organization"] 
    required_keys = ["parameters","object_one","object_two"] 
    required_headers_keys = ['Init-Country','Channel-Identifier','Unique-Reference','Time-Stamp']

    # Get Data
    data = request.get_json()
    headers = request.headers

    # Validate keys : 
    for key in required_keys : 
        if key not in data.keys() :
            res =  {"errors":obj_validations.errors}
            return make_response(jsonify(res), 400) 

    for key in required_parameters_keys : 
        if key not in data['parameters'].keys() : 
            res =  {"parameters":"{} key is requierd.".format(key)}
            return make_response(jsonify(res), 400) 

    for key in required_headers_keys : 
        if key not in headers.keys() : 
            return {"headers":"{} key is requierd.".format(key)}


    ## Headers
    init_country = headers['Init-Country']
    channel_identifier = headers['Channel-Identifier']
    unique_reference = headers['Unique-Reference']
    time_stamp = headers['Time-Stamp']

    ## Body
    index = data['parameters']['party_type'].lower().strip()
    pre_processing=data['parameters']['pre_processing']
    #weight_type = data['parameters']['weight_type']
    object_one=data['object_one']
    object_two=data['object_two']

    # Validate Data 
    obj_validations  = validations()
    index = obj_validations.validate_index(index)
    pre_processing = obj_validations.validate_pre_processing(pre_processing)
    object_one = obj_validations.validate_input_object(index =index ,_object = object_one)
    object_two = obj_validations.validate_input_object(index =index ,_object = object_two)

    # Check errors
    if len (obj_validations.errors) > 0 : 
        return  make_response(jsonify({"errors":obj_validations.errors}), 400)

    # Get Results
    obj_Operations = Operations()
    output,status = obj_Operations.compare(
        index = index ,
        #weight_type=weight_type,
        pre_processing = pre_processing ,
        object_one = object_one,
        object_two= object_two
        )
    result = dict()
    for key , value in output.items() :

        if type (value) == dict :
            result[key] = value['similarity_ratio']
        else : 
            result[key] = value
            
    # Add to log 
    add_obj_log = obj_Operations.add_to_log(headers=headers, status=status, operation="compare" , party_type=None, party_id=None)
    return make_response(jsonify(result),status)


@app.route('/phonotics/feedback/<key>', methods=['POST'])
def Feedback(party_id):
    data = request.get_json()
    headers = request.headers

    index=data['parameters']['index']
    update_object=data['parameters']['update_object']
    result = ''
    return result





@app.route('/phonotics/log/', methods=['GET'])
def Log():

    headers = request.headers

    obj_Operations = Operations()
    result , status = obj_Operations.get_log()

    # Add to log 
    add_obj_log = obj_Operations.add_to_log(headers=headers, status=status, operation="log" , party_type=None , party_id=None)
    return make_response(jsonify(result), status)




@app.errorhandler(400)
def handle_400_error(_error):
    headers = request.headers
    obj_Operations = Operations()
    # Add to log 
    add_obj_log = obj_Operations.add_to_log(headers=headers, status=400, operation="Misunderstood" , party_type=None , party_id=None)

    """Return a http 400 error to client"""
    return make_response(jsonify({'error': 'Misunderstood'}), 400)


@app.errorhandler(401)
def handle_401_error(_error):
    headers = request.headers
    obj_Operations = Operations()
    # Add to log 
    add_obj_log = obj_Operations.add_to_log(headers=headers, status=401, operation="Unauthorised" , party_type=None , party_id=None)

    """Return a http 401 error to client"""
    return make_response(jsonify({'error': 'Unauthorised'}), 401)


@app.errorhandler(404)
def handle_404_error(_error):
    headers = request.headers
    """Return a http 404 error to client"""
    return make_response(jsonify({'error': 'Not found'}), 404)


@app.errorhandler(500)
def handle_500_error(_error):
    headers = request.headers
    """Return a http 500 error to client"""
    return make_response(jsonify({'error': 'Server error'}), 500)
    

if __name__ == "__main__": 
    
    PARSER = argparse.ArgumentParser(
        description="Arab-Bank-Phonotics")

    PARSER.add_argument('--debug', action='store_true',
                        help="Use flask debug/dev mode with file change reloading")
    ARGS = PARSER.parse_args()

    PORT = int(os.environ.get('PORT', 5000))
    HOST = "127.0.0.1"
    DEBUG = True 

    if ARGS.debug:
        print("Running in debug mode")
        CORS = CORS(APP)
        app.run(host=HOST, port=PORT, debug=DEBUG)
    else:
        app.run(host=HOST, port=PORT, debug=DEBUG)





"""
# -----------------------------------------------------------------------------

@app.route('/phonotics/search_name_english/', methods=['POST'])
def phoneticsSearchenglish():    
    name=request.form["name"]
    index=request.form["index"]
    print (name )
    print (index )
    jsonResponce = Operations().PhoneticsSearchNameFunc(name,index,'en')   
    return jsonify(jsonResponce)

# -----------------------------------------------------------------------------  

@app.route('/phonotics/Delete/', methods=['DELETE'])
def phoneticsDelete():    
    index=request.form["index"]
    id = request.form["id"]
    result=Operations().PhoneticsDeleteFunc(index , id)
    return result['result']

# -----------------------------------------------------------------------------

@app.route('/phonotics/Add/', methods=['post'])
def phoneticsAdd():    
    arabicname=request.form["arabicname"]
    englishname=request.form["englishname"]
    index=request.form["index"]
    result=Operations().PhoneticsAddFunc(index , arabicname, englishname )
    return result['result']

# -----------------------------------------------------------------------------

@app.route('/phonotics/update/', methods=['post'])
def phoneticsUpdate():    
    index=request.form["index"]
    ids = request.form["id"]
    field=request.form["field"]
    update=request.form["update"]
    result=Operations().PhoneticsUpdateFunc(index , ids, field ,update)
    return result['result']

# -----------------------------------------------------------------------------

@app.route('/phonotics/search_address_arabic/', methods=['POST'])
def phoneticsSearcharabicAddress():    
    address=request.form["address"]
    index=request.form["index"]
    jsonResponce=Operations().phoneticsSearchAddressFunc(index , address ,'ar' )
    return jsonify(jsonResponce)    
    
# -----------------------------------------------------------------------------

@app.route('/phonotics/search_address_english/', methods=['POST'])
def phoneticsSearchenglishAddress():    
    address=request.form["address"]
    index=request.form["index"]
    jsonResponce=Operations().phoneticsSearchAddressFunc(index , address ,'en' )
    return jsonify(jsonResponce)


# -----------------------------------------------------------------------------
@app.route('/phonotics/Add_address/', methods=['post'])
def phoneticsAddAddress():    
    arabicAddress=request.form["arabicAddress"]
    englishAddress=request.form["englishAddress"]
    index=request.form["index"]
    result = Operations().PhoneticsAddAddressFunc(index,arabicAddress,englishAddress)
    return result['result']

# --------------------------------Mearge---------------------------------------

@app.route('/phonotics/mearge/', methods=['post'])
def phoneticsMearge():    
    if(request.data):
        jdata = request.get_json()
    else : 
        return 'Please send data'
    
    target=jdata["target"]        
    data= jdata["data"]
    index=jdata["index"]
    result = Operations().phoneticsMeargeFunc(index,data,target)
    return "merged " + str(result) + " rows" 

#-----------------------------------------------------------------------------

@app.route('/phonotics_multi/', methods=['POST'])
def phoneti_multi_index_multi_fields():
    if request.data :
        res = request.get_json()
    else : 
        return 'Please send data'
    result = Operations().PhonetiMultiIndexMultiFields(res)
    return result

#-----------------------------------------------------------------------------



@app.route('/phonotics_all', methods=['GET'])
def phoneticsAll():
    result = Operations().PhoneticsAllFunc()
    return result

#------------------------------------------------------------------------------

@app.route('/phonotics_search_id', methods=['GET'])
def phonotics_search_id():
    if(request.data):
        jdata = request.get_json()
    else:
        return "please send Data"   
    index=jdata["index"]
    field=jdata["field"]
    number=jdata["number"]
    result = Operations().PhonoticsSearchIdFunc(index,field,number)
    return result  

#------------------------------------------------------------------------------


@app.route('/phonotics/Add_Number/', methods=['post'])
def phonetics_add_number():    

    if(request.data):
        jdata = request.get_json()
    else:
        return "please send Data"  

    index=jdata["index"]
    phone_number=jdata["phone_number"]
    national_id=jdata["national_id"]
    result =  Operations().PhoneticsAddNumberFunc(index,phone_number,national_id)
    return result['result']  
"""
#------------------------------------------------------------------------------


