import datetime

import numpy as np
from flask import Flask as _Flask # to resolve : {TypeError}Object of type int64 is not JSON serializable
from flask.json import JSONEncoder as _JSONEncoder # to resolve : {TypeError}Object of type int64 is not JSON serializable

################# PROBLEM SOLVING ##########
# to resolve : {TypeError}Object of type int64 is not JSON serializable
class FlaskJSONEncoder(_JSONEncoder):
    def default(self,obj):
        if isinstance(obj,(np.integer,np.floating,np.bool_)):
            return obj.item()
        elif isinstance(obj,np.ndarray):
            return obj.tolist()
        elif isinstance(obj,(datetime.datetime,datetime.timedelta)):
            return obj.__str__()
        else:
            return super(FlaskJSONEncoder,self).default(obj)
class Flask(_Flask):
    json_encoder = FlaskJSONEncoder
###########

