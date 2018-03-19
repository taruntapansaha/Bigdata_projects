#!/usr/bin/env python
import pydoop.hdfs as hdfs
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
import re
import sys

class Mapper(api.Mapper):

    def map(self, context):

        words = context.value.split()
        filename = context.getInputSplit();
        filename = context.input_split.filename;
        for w in words:
            word = re.sub('[^A-Za-z]+', '', w);
            if(word):
                context.emit(word.lower(), filename)

class Reducer(api.Reducer):
    def reduce(self, context):
        context.emit(context.key, len(list(set(context.values))))


def __main__():
    pp.run_task(pp.Factory(Mapper, Reducer))