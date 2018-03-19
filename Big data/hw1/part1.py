#!/usr/bin/env python
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
import sys
import re

class Mapper(api.Mapper):

    def map(self, context):

        words = context.value.split()
        filename = context.input_split.filename;
        for w in words:
            context.emit((filename+ " " + (re.sub('[^A-Za-z]+', '', w))).lower(), 1)

class Reducer(api.Reducer):
    def reduce(self, context):
        s = sum(context.values)
        context.emit(context.key, s)

def __main__():
    pp.run_task(pp.Factory(Mapper, Reducer))