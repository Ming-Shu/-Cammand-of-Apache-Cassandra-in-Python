from cassandra.cluster import Cluster
from datetime import datetime
from time import gmtime
import time

if __name__ == "__main__":
    cluster = Cluster(['CASSANDRA_IP'],port=9042)
    session = cluster.connect('Keyspace_Name',wait_for_all_pools=True)
    session.execute('USE Keyspace_Name')#run cqlsh command,open keyspace
    rows = session.execute('SELECT * FROM Table_Name') #select a table
    for row in rows:
        unix_timestamp = int(row.ts)# ts is key in table
        local_time = time.localtime(unix_timestamp/1000.0)

        if time.strftime("%Y-%m-%d",local_time) =='2021-09-17':
          print('delete data ...')    
          session.execute('DELETE  FROM Table_Name WHERE entity_type = ' +'\''+str(row.entity_type)+'\''+'and entity_id ='+str(row.entity_id) + 'and key='+'\''+str(row.key)+'\'')# entity_type ,entity_id and key is key in table
        else : 
          date = time.strftime("%Y-%m-%d %H:%M:%S",local_time)  
        print('date',date)
     
    

