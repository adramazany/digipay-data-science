""" dest_card_blacklist :
    4/5/2022 3:53 PM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

from config import config
from rules.abstract_rule import AbstractRule


class DestCardBlacklist(AbstractRule):
    blacklist:list()

    def __init__(self):
        with open(config.dest_card_blacklist_path) as f:
            blacklist = f.read().splitlines()


    def exec(self, dest_card_prefix, dest_card_postfix):

        pass