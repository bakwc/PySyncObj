from argparse import ArgumentParser


class Parser(object):
    def __init__(self):
        self.__parser = ArgumentParser()
        self.__parser.add_argument('-conn', action='store', dest='connection', help='adress to connect')
        self.__parser.add_argument('-pass', action='store', dest='password', help='cluster\'s password')
        self.__parser.add_argument('-status', action='store_true', help='send command \'status\'')
        self.__parser.add_argument('-add', action='store', dest='add', help='send command \'add\'')
        self.__parser.add_argument('-remove', action='store', dest='remove', help='send command \'remove\'')

    def parse(self, args):
        return self.__parser.parse_args(args)
