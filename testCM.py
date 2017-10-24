from countMinSketch import *

width = 10
depth = 3
test = countMinSketch(width, depth)
test.increment("two")
test.increment("four")

print "testing countm"
print test.estimate("four")


width = 10
depth = 3
test2 = countMinSketch(width, depth)
test2.increment("two")
test2.increment("four")

sketch = test2.merge(test)
print sketch.estimate("four")
