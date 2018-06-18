from functions import upload_file,download_file
import shelve, time
from operator import add
from collections import Counter

def lambda_handler(event, context):
	arxiu='MapResult'
	bucket_name="mapreducerenricalvaro"
	
	#n = int(''.join(str(digit) for digit in filter(lambda x: x>='0' and x<='9', list(event['Records'][0]['s3']['object']['key']))[0] )) #num mapper
	n = filter(lambda x: x>='0' and x<='9', list(event['Records'][0]['s3']['object']['key']))[0]; #num mappers
	
	#descarregar arxiu
	dictionaries=[]
	for i in range(0,int(n)):
		download_file(arxiu+str(i)+".shlf",bucket_name)
		dictionaries.append(shelve.open("/tmp/"+arxiu+str(i)+'.shlf')[arxiu+str(i)])

	time1=time.time()
	#funcio de reduce
	reduct={}
	reduct=dict(reduce(add, (Counter(dict(x)) for x in dictionaries)))
	print "Temps Reducer"+str(time.time()-time1)
	
	#escriure a disc serialitzat
	arxiu_pujar='Reduce'
	shelf=shelve.open("/tmp/"+arxiu_pujar+".shlf")
	shelf[arxiu_pujar]=reduct
	shelf.close()
	
	#carregar arxiu
	upload_file(arxiu_pujar+".shlf",bucket_name)
	
	return 'Reducer done!'
