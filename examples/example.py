# Client to establish a connection to MictlanX Storage System (MSS).
from mictlanx.client import Client
# Get numpy to create a matrix
import numpy as np
D = np.ones((10,10))
# Establish a connection to the master storage node of the MSS on the localhost:6001.
c1 = Client(hostname ="localhost",port=6001)
# Put a matrix using the "matrix-0" unique identifier. 
c1.put_matrix(id ="matrix-0",matrix = D)
# Get the matrix with the "matrix-0" identifier stored temporarily in /sink (delete flag must be set to True) .
_,matrix = c1.get_matrix(id ="matrix-0",sink_path="/sink", delete = True) 
print(matrix)


