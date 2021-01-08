#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
After auditing is complete the next step is to prepare the data to be inserted into a SQL database.
To do so you will parse the elements in the OSM XML file, transforming them from document format to
tabular format, thus making it possible to write to .csv files.  These csv files can then easily be
imported to a SQL database as tables.

The process for this transformation is as follows:
- Use iterparse to iteratively step through each top level element in the XML
- Shape each element into several data structures using a custom function
- Utilize a schema and validation library to ensure the transformed data is in the correct format
- Write each data structure to the appropriate .csv files

We've already provided the code needed to load the data, perform iterative parsing and write the
output to csv files. Your task is to complete the shape_element function that will transform each
element into the correct format. To make this process easier we've already defined a schema (see
the schema.py file in the last code tab) for the .csv files and the eventual tables. Using the 
cerberus library we can validate the output against this schema to ensure it is correct.

## Shape Element Function
The function should take as input an iterparse Element object and return a dictionary.

### If the element top level tag is "node":
The dictionary returned should have the format {"node": .., "node_tags": ...}

The "node" field should hold a dictionary of the following top level node attributes:
- id
- user
- uid
- version
- lat
- lon
- timestamp
- changeset
All other attributes can be ignored

The "node_tags" field should hold a list of dictionaries, one per secondary tag. Secondary tags are
child tags of node which have the tag name/type: "tag". Each dictionary should have the following
fields from the secondary tag attributes:
- id: the top level node id attribute value
- key: the full tag "k" attribute value if no colon is present or the characters after the colon if one is.
- value: the tag "v" attribute value
- type: either the characters before the colon in the tag "k" value or "regular" if a colon
        is not present.

Additionally,

- if the tag "k" value contains problematic characters, the tag should be ignored
- if the tag "k" value contains a ":" the characters before the ":" should be set as the tag type
  and characters after the ":" should be set as the tag key
- if there are additional ":" in the "k" value they and they should be ignored and kept as part of
  the tag key. For example:

  <tag k="addr:street:name" v="Lincoln"/>
  should be turned into
  {'id': 12345, 'key': 'street:name', 'value': 'Lincoln', 'type': 'addr'}

- If a node has no secondary tags then the "node_tags" field should just contain an empty list.

The final return value for a "node" element should look something like:

{'node': {'id': 757860928,
          'user': 'uboot',
          'uid': 26299,
       'version': '2',
          'lat': 41.9747374,
          'lon': -87.6920102,
          'timestamp': '2010-07-22T16:16:51Z',
      'changeset': 5288876},
 'node_tags': [{'id': 757860928,
                'key': 'amenity',
                'value': 'fast_food',
                'type': 'regular'},
               {'id': 757860928,
                'key': 'cuisine',
                'value': 'sausage',
                'type': 'regular'},
               {'id': 757860928,
                'key': 'name',
                'value': "Shelly's Tasty Freeze",
                'type': 'regular'}]}

### If the element top level tag is "way":
The dictionary should have the format {"way": ..., "way_tags": ..., "way_nodes": ...}

The "way" field should hold a dictionary of the following top level way attributes:
- id
-  user
- uid
- version
- timestamp
- changeset

All other attributes can be ignored

The "way_tags" field should again hold a list of dictionaries, following the exact same rules as
for "node_tags".

Additionally, the dictionary should have a field "way_nodes". "way_nodes" should hold a list of
dictionaries, one for each nd child tag.  Each dictionary should have the fields:
- id: the top level element (way) id
- node_id: the ref attribute value of the nd tag
- position: the index starting at 0 of the nd tag i.e. what order the nd tag appears within
            the way element

The final return value for a "way" element should look something like:

{'way': {'id': 209809850,
         'user': 'chicago-buildings',
         'uid': 674454,
         'version': '1',
         'timestamp': '2013-03-13T15:58:04Z',
         'changeset': 15353317},
 'way_nodes': [{'id': 209809850, 'node_id': 2199822281, 'position': 0},
               {'id': 209809850, 'node_id': 2199822390, 'position': 1},
               {'id': 209809850, 'node_id': 2199822392, 'position': 2},
               {'id': 209809850, 'node_id': 2199822369, 'position': 3},
               {'id': 209809850, 'node_id': 2199822370, 'position': 4},
               {'id': 209809850, 'node_id': 2199822284, 'position': 5},
               {'id': 209809850, 'node_id': 2199822281, 'position': 6}],
 'way_tags': [{'id': 209809850,
               'key': 'housenumber',
               'type': 'addr',
               'value': '1412'},
              {'id': 209809850,
               'key': 'street',
               'type': 'addr',
               'value': 'West Lexington St.'},
              {'id': 209809850,
               'key': 'street:name',
               'type': 'addr',
               'value': 'Lexington'},
              {'id': '209809850',
               'key': 'street:prefix',
               'type': 'addr',
               'value': 'West'},
              {'id': 209809850,
               'key': 'street:type',
               'type': 'addr',
               'value': 'Street'},
              {'id': 209809850,
               'key': 'building',
               'type': 'regular',
               'value': 'yes'},
              {'id': 209809850,
               'key': 'levels',
               'type': 'building',
               'value': '1'},
              {'id': 209809850,
               'key': 'building_id',
               'type': 'chicago',
               'value': '366409'}]}
"""

import csv
import codecs
import collections
import pprint
import re
import xml.etree.cElementTree as ET
import pandas as pd

import cerberus

import schema

OSM_PATH = "Lasvegas.osm"
USPS_STREET = "USPS Street Abbrev.csv"
CITIES_LIST = "Cities_List.csv"

NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
street_type_re=re.compile(r'\b\S+\.?$',re.IGNORECASE)

Cross_Reference = pd.DataFrame()
Cross_Reference_Cities = pd.DataFrame()

SCHEMA = schema.schema

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']


# ================================================== #
#               Audit Functions                      #
# ================================================== #
####             AUDITING STREET TYPES            ####

#Build list of USPS Names for Standardization (CSV drawn from list of address validations on USPS web site-Aimee's function)
def readindata(datafile):

    data = pd.read_csv(datafile)
    return data

def savedata(df,datafile):
    saveddata = df.to_csv(datafile,index=False)
    return saveddata

def createStCR():
    StCR=readindata(USPS_STREET)
    return StCR

def createCityCR():
    CityCR=readindata(CITIES_LIST)
    return CityCR
    
#Determine street names (from case study)
def is_street_name(elem):
    return (elem.attrib['k']=="addr:street")

#Find weirdo street names (from case study with slight modifications)
def audit_street_type(street_types, street_name,CR):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if str.upper(street_type) not in CR.values:
           street_types[street_type].add(street_name)
        return street_types

#Test the dataset to see how many variations appear (Aimee's function)        
def count_street_name():
    namecount = 0
    for event, elem in ET.iterparse(OSM_PATH,events=('start',)):
        if elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    namecount = namecount +1
    print "Total Name Count: ", namecount

#Function to see how many "weirdo" names are generated in auditing. (Aimee's function)
def count_street_type(st):
    stcount = 0
    for row in st:
        stcount = stcount +1
    print "Total Street Type Count", stcount

#Audit function for street types (from case study with slight modification)
def audit(osmfile,CR):
    osm_file = open(osmfile, "r")
    street_types = collections.defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'],CR)
                    
    osm_file.close()   
    return street_types


#Cycle through list of 'weirdos' and add to cross-reference with better name. (Aimee's function)
def addmappings(st_list,CR):
    addedmappings=pd.DataFrame(columns=['FullName', 'CommonName', 'USPSName'])
    
    for st in st_list:
         am_length=len(addedmappings)
         st_upper=str.upper(st)
         question="Should we map ", st, "? Type Y to map."
         map = raw_input(question)
         if map == "Y":
            newvalue = raw_input("What should the mapped value be?")
            addedmappings.loc[am_length]=(newvalue,st_upper,st_upper)
            
         elif map == "N":
            next
         else:
             next
    
    CR=CR.append(addedmappings).reset_index(drop=True)
    savedata(CR,USPS_STREET)
    return CR


#Function to update "weirdo" street names (Aimee's function - all forced to upper per my preference).  
def update_name(name, CR):
    m = street_type_re.search(name)
    new_name=""
    if m:        
        street_type = m.group()
        street_type_upper=str.upper(street_type)
          
        if street_type_upper in CR["CommonName"].values:
           new_value_cn=CR.loc[CR["CommonName"] == street_type_upper, "FullName"].iloc[0]
           new_name=str.upper(name.replace(street_type_upper,new_value_cn))
           
        elif str.upper(street_type) in CR["USPSName"].values:
           new_value_un=CR.loc[CR["USPSName"] == str.upper(street_type), "FullName"].iloc[0]
           new_name=str.upper(name.replace(street_type_upper,new_value_un))
           
        else:
           new_name=str.upper(name)
                
    else:
         new_name=str.upper(name)

    return new_name

#Helper function for testing to view results of street name updates without running full process map. (Aimee's function)
def dispnewnames(osmfile,CR):
    osm_file = open(osmfile, "r")
    new_names = collections.defaultdict(set)
    
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    street_name=tag.attrib['v']
                    new_names[street_name].add(update_name(street_name,CR))  
    osm_file.close()
    print new_names
    print len(new_names)
    return new_names

####             AUDITING CITIES            ####

#What do the cities look like - are they fairly normalized?
def citieslist(osmfile):
    osm_file = open(osmfile, "r")
    attriblist = collections.defaultdict(set)
    
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if tag.attrib["k"]=="addr:city":
                    attriblist[tag.attrib["k"]].add(tag.attrib["v"])

    pprint.pprint(attriblist)
                    
    osm_file.close()   
    return attriblist



#Function to cycle through all listed cities, identify ones that need changing, and desired replacement values.
def buildcitiescrossreference(cities):
    citiescrossreference = readindata(CITIES_LIST)
  
    for city in cities["addr:city"]:
        if str.upper(city) not in citiescrossreference:
            city_upper=str.upper(city)
            new_row={}
            prompt= "Do you want to keep ",city_upper,"? Enter y or Y to keep."
            response=raw_input(prompt)
            if str.upper(response)=="Y":
                new_row={'OriginalName':city_upper, 'NewName':city_upper}
                citiescrossreference=citiescrossreference.append(new_row,ignore_index=True)
                
            elif response!="Y":
                prompt2 = "What should be substituted for ",city_upper," ? (entries are not case-sensitive)"
                newcityname = str.upper(raw_input(prompt2))
                new_row={'OriginalName':city_upper, 'NewName':newcityname}
                citiescrossreference=citiescrossreference.append(new_row, ignore_index=True)
    
    writeprompt="Do you want to write these mappings to file? Type 'y' or 'Y' to write:"
    write=raw_input(writeprompt)
    if str.upper(write)=="Y":
       savedata(citiescrossreference,CITIES_LIST) 
    
    return citiescrossreference

#update city names with new name from DataFrame build in buildcitiescrossreference function.
def update_city_name(name, CR):
    
    new_city_name=""
         
    if str.upper(name) in CR["OriginalName"].values:
       
       new_city_name=CR.loc[CR["OriginalName"] == name, "NewName"].iloc[0]
       new_city_name=name.replace(name,new_city_name)
    
    else:
       new_city_name=name
    
    return new_city_name

#Function to run all city names through the update process and display results (to ensure they are as desired).
def dispnewcitynames(osmfile,CR):
    osm_file = open(osmfile, "r")
    new_city_names = collections.defaultdict(set)
    
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                 if tag.attrib["k"]=="addr:city":
                    city_name=tag.attrib['v']
                    new_city_names[city_name].add(update_city_name(city_name,CR))  
    osm_file.close()
    print new_city_names
    print len(new_city_names)
    return new_city_names

#Function to handle processing of subtags in shape_element. (Aimee's function)
def handle_tags(key_name,value_name,problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    value_name=value_name.upper()           
    if not PROBLEMCHARS.search(key_name):
        if ":" in key_name:
            
            key_split=key_name.split(':',1)
            key_name=key_split[0]
            type_name=key_split[1]
            
            if type_name=="street":
            
               value_name=update_name(value_name, Cross_Reference) 
                                        
            elif type_name=="city":
               
               value_name=update_city_name(value_name,Cross_Reference_Cities)
               
               
        elif ":" not in key_name:
            
            type_name=default_tag_type
            
    key_value_type = collections.namedtuple("key_value_type", ["key", "value", "type"])
    return key_value_type(key_name,value_name,type_name)   
 
#Shape element function from case study, finished by Aimee.      
def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""

    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []  # Handle secondary tags the same way for both node and way elements

    # YOUR CODE
    nodeid=""
    wayid=""
    index=0
    tag_values={}
    way_node_values={}
    
    if element.tag == 'node':

        for field in NODE_FIELDS:
            node_attribs[field]=element.attrib[field].upper()
            if field =="id":
                nodeid=element.attrib[field]
                        
        for element in element.iter('tag'):
            tag_values['id']=nodeid
            key_value_type=handle_tags(element.attrib['k'],element.attrib['v'])
            tag_values['key']=key_value_type.key
            tag_values['value']=key_value_type.value
            tag_values['type']=key_value_type.type
            tag_values_c=tag_values.copy()
            tags.append(tag_values_c)
                               
        return {'node': node_attribs, 'node_tags': tags}
    
    if element.tag == 'way':
        for field in WAY_FIELDS:
            way_attribs[field]=element.attrib[field].upper()
            if field=="id":
                wayid=element.attrib[field]
            
            
        for element in element.iter('tag'):
            tag_values['id']=wayid
            key_value_type=handle_tags(element.attrib['k'],element.attrib['v'])
            tag_values['key']=key_value_type.key
            tag_values['value']=key_value_type.value
            tag_values['type']=key_value_type.type 
            tag_values_c=tag_values.copy()
            tags.append(tag_values_c)
        
            
        for element in element.iter('nd'):
            way_node_values['id']=wayid
            way_node_values['node_id']=element.attrib['ref'].upper()
            way_node_values['position']=index
            index=index+1
            way_node_values_c=way_node_values.copy()
            way_nodes.append(way_node_values_c)
            
        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}


# ================================================== #
#               Helper Functions                     #
# ================================================== #
#Function from case study.
def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()

#Helper function to show results of processing ways and nodes tags without running full program. (Aimee's function)            
def showdictionaryvalues(osmfile):
   osm_file = open(osmfile, "r")
   for element in get_element(osm_file, tags=('node','way')):
              el = shape_element(element)
              print el
   osm_file.close()
   
   return el


#Function from case study.
def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)
        
        raise Exception(message_string.format(field, error_string))

#Function from case study.
class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'wb') as nodes_file, \
          codecs.open(NODE_TAGS_PATH, 'wb') as nodes_tags_file, \
          codecs.open(WAYS_PATH, 'wb') as ways_file, \
          codecs.open(WAY_NODES_PATH, 'wb') as way_nodes_file, \
          codecs.open(WAY_TAGS_PATH, 'wb') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])


if __name__ == '__main__':
#     # Note: Validation is ~ 10X slower. For the project consider using a small
#     # sample of the map when validating.
#     #Build cross references for cleaning.
    
    Cross_Reference_Cities=createCityCR()
    Cross_Reference=createStCR()
    
    AddtoCRSPrompt="Do you want to add mapping to the Cross Reference files? Type Y to add mappings."
    AddtoCR=raw_input(AddtoCRSPrompt)
    
    if str.upper(AddtoCR)=="Y":
        Cross_Reference=addmappings(audit(OSM_PATH,Cross_Reference),Cross_Reference)
        Cross_Reference_Cities= buildcitiescrossreference(citieslist(OSM_PATH))
#    showdictionaryvalues(OSM_PATH)    
#     #Run full data/file processing subroutine.
    process_map(OSM_PATH, validate=False)
