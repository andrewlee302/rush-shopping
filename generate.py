#!/usr/bin/env python

import random
import os
import sys


USER_CSV = "data/users.csv"
ITEM_CSV = "data/items.csv"
INIT_ROOT_MONEY = 0
INIT_MONEY = 200
NUM_USERS = 2000
NUM_ITEMS = 2000

if not os.path.exists("data"):
    os.mkdir("data")
elif not os.path.isdir("data"):
    print "It's not a directory, ./data."
    exit(1)

# Id for normal users starts from 1.
print "Generate %d users -> %s" %(NUM_USERS, USER_CSV)
with open(USER_CSV, 'w') as f:
    # root
    f.write("%d,%s,%d,%d\n" % (0, "root", 0, INIT_ROOT_MONEY))
    for i in range(1, NUM_USERS):
        name = "andrew%d" %i
        password = i
        f.write("%d,%s,%d,%d\n" % (i, name, password, INIT_MONEY))

# Id for items starts from 1.
print "Generate %d items -> %s" %(NUM_ITEMS, ITEM_CSV)
with open(ITEM_CSV, 'w') as f:
    for i in range(1, NUM_ITEMS+1):
        price = random.randint(1,101)
        count = random.randint(1,21) * 5
        f.write("%d,%d,%d\n" % (i, price, count))
