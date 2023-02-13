# Importa el cliente
from mictlanx.mictlanx import Client
# Importa numpy unicamente para pruebas.
import numpy as np

D = np.ones((10,10))
# Crea un cliente y se conecta con el sistema que esta en el hostname:port asignado en sus kwargs.
c1 = Client(hostname ="localhost",port=6001)
# Guardar una matriz con el identificador "matrix-0" , debe de ser un NUMPY ARRAY.
c1.put_matrix(id ="matrix-0",matrix = D)
# Traer una matriz con el identificador "matrix-0" y la guarda en /sink y despues la borra si delete es verdadero.
_,matrix = c1.get_matrix(id ="matrix-0",sink_path="/sink", delete = True)
# imprime la matriz que guarde en el sistema y que obtuve utilizando get_matrix. 
print(matrix)


