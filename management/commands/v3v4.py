'''
ARCHES - a program developed to inventory and manage immovable cultural heritage.
Copyright (C) 2013 J. Paul Getty Trust and World Monuments Fund

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
'''

"""This module contains commands for building Arches."""

import os
import csv
from datetime import datetime
from optparse import make_option
from django.core.management.base import BaseCommand, CommandError
from arches.app.models.models import Concepts, RelatedResource
from arches.app.models.concept import Concept
from _archesjson import JsonWriter
from _skos import SKOSWriter


class Command(BaseCommand):
    """
    Commands for managing the loading and running of packages in Arches

    """
    
    option_list = BaseCommand.option_list + (
        make_option('-o', '--operation', action='store', type='choice',
            choices=['export-skos', 'export-resources', 'export-relations'],
            help='Export commands to prepare for migration to v4'),
        make_option('-f', '--format', action='store', type='choice',
            choices=['JSON', 'JSONL'],
            help='Export commands to prepare for migration to v4'),
        make_option('--name', action='store', default='arches',
            help='Name of the concept scheme to export'),
        make_option('--overwrite', action='store_true', default=False,
            help='Overwrite any existing file with the output of this command.'),
        make_option('--split', action='store_true', default=False,
            help='Split the resources into different files by resource type "\
            "during resource export.'),
        make_option('--multiprocessing', action='store_true', default=False,
            help='Use multiprocessing during export (JSON only).'),
    )

    def handle(self, *args, **options):

        if options['operation'] == 'export-skos':
            self.export_skos(options['name'], overwrite=options['overwrite'])
        if options['operation'] == 'export-resources':
            self.export_resources(format=options['format'], overwrite=options['overwrite'],
                split_types=options['split'], multiprocessing=options['multiprocessing'])
        if options['operation'] == 'export-relations':
            self.export_relations()
    
    def make_file_name(self, prefix, subject, extension):
        
        name = datetime.now().strftime(
            "{}-{}-%Y-%m-%d.{}".format(prefix, subject, extension))
        return name

    def export_skos(self, scheme_name, overwrite=False):
        """
        Exports the specified concept scheme to an .xml SKOS file
        """
        print "Exporting concept scheme: {}".format(scheme_name)
        
        outfile = datetime.now().strftime(
            "v3scheme-{}-%Y-%m-%d.xml".format(scheme_name.lower()))
        outfile = self.make_file_name("v3scheme",scheme_name.lower(),"xml")
        print "Output file: {}".format(outfile)
        
        # check if file exists and prompt for whether to overwrite it
        if os.path.isfile(outfile) and overwrite is False:
            confirm = raw_input("    File already exists. Overwrite? Y/n > ")
            if not confirm.lower().startswith("y") and not confirm == "":
                exit()

        print "\ncollecting concepts...",
        c = Concepts.objects.get(legacyoid__iexact=scheme_name)
        concept_graph = Concept().get(id=c.conceptid, include_subconcepts=True, 
                include_parentconcepts=True, include_relatedconcepts=True,
                depth_limit=None, up_depth_limit=None, lang="en-US")
        print "done"
        print "writing file...",
        output = SKOSWriter().write(concept_graph, format="pretty-xml")

        with open(outfile,"wb") as outf:
            outf.write(output)
        print "done"

    def export_resources(self, format="JSON", split_types=False, overwrite=False, multiprocessing=False):
        """
        Exports resources to archesjson
        """
        if format=="JSON":
            writer = JsonWriter(multiprocessing=multiprocessing)
        elif format=="JSONL":
            writer = JsonWriter(jsonl=True,multiprocessing=multiprocessing)
        
        filename = self.make_file_name("v3resources", "all", format.lower())
        writer.write_resources(filename=filename, split_types=split_types)
    
    def export_relations(self):
        related_resources = [ {
            'RESOURCEID_FROM':rr.entityid1,
            'RESOURCEID_TO':rr.entityid2,
            'RELATION_TYPE':rr.relationshiptype,
            'START_DATE':rr.datestarted,
            'END_DATE':rr.dateended,
            'NOTES':rr.notes} for rr in RelatedResource.objects.all()]

        print related_resources[0]
        relations_file = self.make_file_name("v3relations", "all", "relations")
        with open(relations_file, 'wb') as f:
            csvwriter = csv.DictWriter(f, delimiter='|', fieldnames=[
                'RESOURCEID_FROM','RESOURCEID_TO','START_DATE','END_DATE','RELATION_TYPE','NOTES'])
            csvwriter.writeheader()
            for csv_record in related_resources:
                csvwriter.writerow({k: str(v).encode('utf8') for k, v in csv_record.items()})
