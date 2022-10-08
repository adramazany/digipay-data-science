""" test_make_iter_column_name :
    3/15/2022 5:17 PM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

clean_name = "abcdefghijklmnopqrstuvwxyzABCDE"

# happy path
x="abcdefghijklmnopqrstuvwxyzABCDE"
if x[:29]==clean_name[:29] and x[29:]  :
    print(x[:29],x[29:],x[29:].isnumeric())

# x is less
x="abcdefghijklmnopqrstuvwxyzAB"
if x[:28]==clean_name[:28] and x[29:]  :
    print(x[:28],x[29:],x[29:].isnumeric())

# x last is number
x="abcdefghijklmnopqrstuvwxyzAB_12"
if x[:28]==clean_name[:28] and x[29:]  :
    print(x[29:].isnumeric())

print('%02d' % 331)