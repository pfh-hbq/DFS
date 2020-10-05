import rpyc
import uuid
import os
import sys

from rpyc.utils.server import ThreadedServer

DATA_DIR="./minion/"

class MinionService(rpyc.Service):
  class exposed_Minion():
    blocks = {}

    def exposed_put(self,block_uuid,data,minions):
      with open(DATA_DIR+str(block_uuid),'wb') as f:
        f.write(data)
        print("Save file")
      if len(minions)>0:
        self.forward(block_uuid,data,minions)


    def exposed_get(self,block_uuid):
      print("\nsUID: " + str(block_uuid))
      block_addr=DATA_DIR+str(block_uuid)
      if not os.path.isfile(block_addr):
        print("No such file")
        return None
        
#        f = open(filename, 'rb')

      with open(block_addr, 'rb') as f:
        return f.read()   
 
    def forward(self,block_uuid,data,minions):
      print("8888: forwaring to:")
      print(block_uuid, minions)
      minion=minions[0]
      minions=minions[1:]
      host,port=minion

      con=rpyc.connect(host,port=port)
      minion = con.root.Minion()
      minion.put(block_uuid,data,minions)

    def delete_block(self,uuid):
      pass
      
def parse_command_line_arguments():         # parse command arguments
    if len(sys.argv) == 2:                  # especially ip, port of the server and file name to transfer
        TCP_PORT = int(sys.argv[1])
        return TCP_PORT
    else:
        return -1

if __name__ == "__main__":
  if not os.path.isdir(DATA_DIR): os.mkdir(DATA_DIR)
  TCP_PORT = parse_command_line_arguments()
  t = ThreadedServer(MinionService, port = TCP_PORT)
  t.start()