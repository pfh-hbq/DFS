import rpyc
import sys
import os
import logging

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)

def send_to_minion(block_uuid,data,minions):
  LOG.info("sending: " + str(block_uuid) + str(minions))
  minion=minions[0]
  minions=minions[1:]
  host,port=minion

  con=rpyc.connect(host,port=port)
  minion = con.root.Minion()
  minion.put(block_uuid,data,minions)


def read_from_minion(block_uuid,minion):
  host,port = minion
  con=rpyc.connect(host,port=port)
  minion = con.root.Minion()
  return minion.get(block_uuid)

def get(master,fname):
  name, extension = fname.split('.')
  
  file_table = master.get_file_table_entry(name)
  if not file_table:
    LOG.info("404: file not found")
    return

  new_filename = get_new_filename(name, extension)
  f = open(new_filename, "wb")
  
  for block in file_table:
    for m in [master.get_minions()[_] for _ in block[1]]:
      data = read_from_minion(block[0], m)
      if data:
        f.write(data)
        
        
        print("Get data from HDFS")
#        sys.stdout.write(data)
        break
    else:
        LOG.info("No blocks found. Possibly a corrupt file")
  f.close()

def get_new_filename(name, extension):
    try:
        f = open(name + "." + extension)
        f.close()
        i = 1
        while True:                                                     # try different file names
            try:                                                        # untill find one which is free
                f = open(name + "_copy{}".format(i) + "." + extension)
                f.close()
                i += 1
                continue
            except IOError:
                return name + "_copy{}".format(i) + "." + extension

    except IOError:
        f = open(name + "." + extension, "w+")
        f.close()
        return name + "." + extension
    
def put(master,source,dest):
  size = os.path.getsize(source)
  blocks = master.write(dest,size)
  with open(source, 'rb') as f:
    for b in blocks:
      data = f.read(master.get_block_size())
      block_uuid=b[0]
      minions = [master.get_minions()[_] for _ in b[1]]
      send_to_minion(block_uuid,data,minions)


def main(args):
  con=rpyc.connect("localhost",port=2131)
  master=con.root.Master()
  
  if args[0] == "get":
    get(master,args[1])
  elif args[0] == "put":
    put(master,args[1],args[2])
  else:
    LOG.error("try 'put srcFile destFile OR get file'")


if __name__ == "__main__":
  main(sys.argv[1:])