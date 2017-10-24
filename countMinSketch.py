


import mmh3
import math

class countMinSketch() :

    def __init__(self, width, depth) :
        self.width = width
        self.depth = depth
        "self.seed1
        self.cm = [[0]*width]*depth
    
    def compute_width_depth(delta, epsilon):
        d = 2/ epsilon        "  d represents the depth of the 2D array, or the number of hash functions used
        w = log (1/ delta)    "  w represents the width of the 2D array
        return {"width": w, "depth": d}
    
    "def getIndicies(self, key):
'        key = key.encode("utf-8)
'        hash1 = mmh3.hash(key, self.seed1)
'        hash2 = mmh3.hash(key, self.seed2)
'        w = self.width
'        return ((i, (hash1 +i*hash2)%w) for i in xrange(self.depth))

    def increment(self, key) :
     "     Calculate 'd' hash values for this item where 'd' is the depth of the Count-Min sketch.
     "     Each hash value here will be a number between 0 and 'width'
     "      ...
     "      hashes[1] = get_hash(item)          
     "      ...
      
     "     Increment the corresponding counter which is pointed to by each of the hash values. 
     "     i.e. if hashes[1] was equal to 4, then we would increment counter[4] corresponding to the first row of the  sketch
        d = self.depth
        w = self.width
        for i in range(d):
            index = mmh3.hash(key, i) % w
            self.cm[i][index] =+ 1
        return self


    def estimate(self, key) :
     "     Calculate 'd' hash values for this item similar to what we did for increment.          
     "     Return the minimum value among all the counters these hash values point to.
      
     "     i.e if we had depth = 2 and hashes[1] = 4, and hashes[2] = 2, the we would return the
     "     minimum of counter[4] from the first row and counter[2] from the second row.

        d = self.depth
        w = self.width
        hashArray = []
        for j in range(d):
           index = mmh3.hash(key, i) % w
           hashArray.append(self.cm[i][index])
        return min(hashArray)

    def merge(self, cm2) :
 
        d = self.depth
        w = self.width
        for i to d:
            for j to w:
            self.cm[i][j] += cm2[i][j]
        return self
                         

