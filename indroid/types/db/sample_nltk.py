#!/usr/bin/python -i

####################################
##          Initialize            ##
####################################

import sys, math, operator
import nltk
from gevent.queue import JoinableQueue, Queue, Empty
from collections import Counter, defaultdict
