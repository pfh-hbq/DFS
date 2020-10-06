import rpyc
import uuid
import threading 
import math
import random
import configparser
import signal
import pickle
import sys
import os

from rpyc.utils.server import ThreadedServer

def int_handler(signal, frame):
    pickle.dump((MasterService.exposed_Master.file_table,MasterService.exposed_Master.block_mapping),open('fs.img','wb'))
    sys.exit(0)

def set_conf():
  conf=configparser.ConfigParser()
  conf.readfp(open('dfs.conf'))
  MasterService.exposed_Master.block_size = int(conf.get('master','block_size'))
  MasterService.exposed_Master.replication_factor = int(conf.get('master','replication_factor'))
  minions = conf.get('master','minions').split(',')
  for m in minions:
    id,host,port=m.split(":")
    MasterService.exposed_Master.minions[id]=(host,port)

  if os.path.isfile('fs.img'):
    MasterService.exposed_Master.file_table,MasterService.exposed_Master.block_mapping = pickle.load(open('fs.img','rb'))

class MasterService(rpyc.Service):
  class exposed_Master():
    file_table = {}
    block_mapping = {}
    minions = {}

    block_size = 0
    replication_factor = 0

    def exposed_read(self, fname):
        mapping = self.__class__.file_table[fname]
        return mapping
      
    def exposed_change_filepath(self, old_path, new_path):
        self.__class__.file_table[new_path] = self.__class__.file_table.pop(old_path)
        

    def exposed_write(self, dest, size):
        if self.exists(dest):
            pass # ignoring for now, will delete it later
        
        self.__class__.file_table[dest]=[]

        num_blocks = self.calc_num_blocks(size)
        blocks = self.alloc_blocks(dest,num_blocks)
        return blocks
    
    def exposed_delete_file(self, file_path):
        file_table = self.__class__.file_table[file_path]
        
        for block in file_table:
            for m in [self.exposed_get_minions()[_] for _ in block[1]]:
              data = self.delete_block(block[0], m)
              if data:
                f.write(data)
                
        del self.__class__.file_table[file_path]
        
                
    def delete_block(self, block_uuid, minion):
        host, port = minion
        con = rpyc.connect(host, port = port)
        minion = con.root.Minion()
        
        minion.delete_block(block_uuid)

    def exposed_get_file_table_entry(self, fname):
        if fname in self.__class__.file_table:
            return self.__class__.file_table[fname]
        else:
            return None

    def exposed_get_block_size(self):
        return self.__class__.block_size

    def exposed_get_minions(self):
        return self.__class__.minions

    def calc_num_blocks(self,size):
        return int(math.ceil(float(size)/self.__class__.block_size))

    def exists(self,file):
        return file in self.__class__.file_table

    def alloc_blocks(self, dest, num):
        blocks = []
        for i in range(0, num):
            block_uuid = uuid.uuid1()
            nodes_ids = random.sample(self.__class__.minions.keys(),self.__class__.replication_factor)
            blocks.append((block_uuid,nodes_ids))

            self.__class__.file_table[dest].append((block_uuid,nodes_ids))

        return blocks
    
    def exposed_list(self, source):
        return self.get_subdir(source)
    
#   get all 1 level subdirectories / files of the source directory

    def get_subdir(master, source):
        table = master.__class__.file_table
        
        sub_dirs = set()
        
        for key in table.keys():
#            sub_dirs.add("Key: {} Dir: {}".format(key, source))
            if key.startswith(source):
                sub_dir = key.split(source)[1]
                file = sub_dir.split("/")[1]
                
                sub_dirs.add(file)
            elif "./" == source:
                sub_dir = key.split("/")[0]
                sub_dirs.add(sub_dir)
        
        return sub_dirs
    
#   get stats about file: size, nodes

    def exposed_file_info(self, fname):
        file_table = self.exposed_get_file_table_entry(fname)
        if not file_table:
            LOG.info("404: file not found")
            return
        
        size = 0
        nodes = set()
        
        for block in file_table:
            for m in [self.exposed_get_minions()[_] for _ in block[1]]:
                data = self.read_from_minion(block[0], m)
                host, port = m
                nodes.add("{} : {}".format(host, port))
                if data:
                    size += sys.getsizeof(data)
                    break
                else:
                    LOG.info("No blocks found. Possibly a corrupt file")
          
        size_in_mb = round(size / 1024 / 1024, 2)
        
        info = "File size: {} MB\n".format(size_in_mb) + "\nNodes adresses:\n" + "\n".join(nodes)
        
        return info
        
    def read_from_minion(master, block_uuid, minion):
        host,port = minion
        con=rpyc.connect(host,port=port)
        minion = con.root.Minion()
        return minion.get(block_uuid)

if __name__ == "__main__":
    set_conf()
    signal.signal(signal.SIGINT,int_handler)
    t = ThreadedServer(MasterService, port = 2131)
    t.start()
