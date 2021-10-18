# TCPClient-Server

1. Clone el repositorio en su maquina local. Elija una maquina donde correr el servidor y otra en donde correr el cliente. Este programa tambien puede ser ejecutado ambos programas en la misma maquina.
2. Asegurarse que en el ambiente donde se decida correr la aplicación cuente con python y pip instalados
3. Una vez clonado el repositorio navegue a esa carpeta Server o Client dependiendo de cual va a ejecutar.
4. Descargue los archivos que se van a transferir del siguiente link para el archivo de 100 MB https://www.dropbox.com/s/qc5erqchyhhj84r/File1.mp4?dl=0. Para el archivo de 250 MB use el siguiente link https://www.dropbox.com/s/nawghvadzsy7opk/File2.mp4?dl=0. 
5. Asegurese de copiar estos archivos en la carpeta Server/data/Files. Con los nombres File1.mp4 para el archivo de 100 MB y File2.pm4 para el archivo de 250 MB.
6. Una vez en esa carpeta instale los requirements del cada proyecto. Para eso ejecute "pip install -r requirements.txt"
7. Si esta ejecutando de forma local debe modificar el codigo de tanto el archivo client.py y server.py en las lineas donde se le asigna a la variable "host" la IP debe ir "localhost". Y elegir un puerto que compartan ambos archivos y reemplazar la linea donde se le asigna a la varible "port" el numero de puerto.
8. Si lo esta ejecutando de forma separada debe modificar unicamente el archivo de client.py la linea de "host" con el valor de la IP de la maquina donde se aloje el servidor. Tener en cuenta que los puertos deben ser los mismos en ambos archivos.
9. Una vez hecho esto ya puede ejecutar cualquiera de los dos programas. Para esto ejecute "python client.py" o "python server.py"
10. El el servidor antes de empezar la ejecución le debera indicar el archivo que quiere transferir y la cantidad de clientes.
11. Para el cliente debe ejecutar tantas veces como clientes quiera para su prueba. Es decir si desea 5 clientes debe abrir 5 consolas de comando, por ejemplo.
