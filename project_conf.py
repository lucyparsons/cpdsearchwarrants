import json

class Conf():
    def __init__(self):
        fh = open('project.conf')
        self.__dict__ = json.load(fh)

conf = Conf()
