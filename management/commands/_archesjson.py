
import os
import types
import sys
from django.conf import settings
from django.db import connection
from arches.app.models.models import Entities, EntityTypes
from arches.app.models.resource import Resource
import codecs
from _format import Writer
import json
from arches.app.utils.betterJSONSerializer import JSONSerializer, JSONDeserializer
import csv

import time
from multiprocessing import Pool, TimeoutError, cpu_count
from django.db import connections

# this wrapper function must be outside of the class to be called during the
# multiprocessing operations.
def write_one_resource_wrapper(args):
    return JsonWriter.write_one_resource(*args)

class JsonWriter(Writer):

    def __init__(self, jsonl=False):
        super(JsonWriter, self).__init__()
        self.jsonl = jsonl

    def write_one_resource(self, resource_id):

        try:
            a_resource = Resource().get(resource_id)
            a_resource.form_groups = None
            jsonres = JSONSerializer().serialize(a_resource, separators=(',',':'))
        except Exception as e:
            print "ERROR: in resource", resource_id
            print e
            jsonres = "{}"
        return jsonres

    def write_resources(self, filename, split_types=False):

        # if JSONL file is desired, use this code to write the resource line by
        # line, and also introduce multiprocessing to speed things up.
        
        restypes = [i.entitytypeid for i in EntityTypes.objects.filter(isresource=True)]
        start = time.time()
        total_count = 0

        if self.jsonl is True:

            process_count = cpu_count()
            print "cpu count:", cpu_count()
            print "number of parallel processes:", process_count
            pool = Pool(cpu_count())

        ## if there is only one output file, open it here
        if split_types is False:
            outfile = filename
            openout = open(outfile, "w")

        json_resources = []
        for restype in restypes:

            if split_types is True:
                outfile = filename.replace("all", restype)
                openout = open(outfile, "w")

            resources = Entities.objects.filter(entitytypeid=restype)
            resct = len(resources)
            total_count += resct

            if resct == 0:
                print "Writing {} {} resources".format(resct, restype)
                continue

            print "Writing {} {} resources --> {}".format(resct, restype,
                outfile)

            if self.jsonl is True:
                resids = [r.entityid for r in resources]

                for conn in connections.all():
                    conn.close()

                joined_input = [(self,r) for r in resids]
                for res in pool.imap(write_one_resource_wrapper, joined_input):
                    openout.write(res+"\n")

            else:
                errors = []
                for resource in resources:
                    try:
                        a_resource = Resource().get(resource.entityid)
                        a_resource.form_groups = None
                        json_resources.append(a_resource)
                    except Exception as e:
                        if e not in errors:
                            errors.append(e)
                if len(errors) > 0:
                    print errors[0], ':', len(errors)
                
                if split_types is True: 
                    openout.write((JSONSerializer().serialize({'resources':json_resources},
                    separators=(',',':'))))
                    json_resources = []

        if split_types is False:
            if self.jsonl is False:
                openout.write((JSONSerializer().serialize({'resources':json_resources},
                    separators=(',',':'))))
            openout.close()
        
        print "\n{} resources exported".format(total_count)
        print "elapsed time:", time.time()-start

class JsonReader():

    def validate_file(self, archesjson, break_on_error=True):
        pass

    def load_file(self, archesjson):
        resources = []
        with open(archesjson, 'r') as f:
            resources = JSONDeserializer().deserialize(f.read())
        return resources['resources']