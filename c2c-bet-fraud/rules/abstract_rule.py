""" abstract_rule :
    4/5/2022 3:54 PM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

from abc import abstractmethod


class AbstractRule:

    @abstractmethod
    def exec(self, *args, **kwargs):
        pass

