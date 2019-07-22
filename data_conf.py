#!/usr/bin/python3

import json

class Conf():
    def __init__(self):
        fh = open('data.conf')
        self.__dict__ = json.load(fh)

data_conf = Conf()
