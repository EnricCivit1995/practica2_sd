from functions import upload_file,download_file
import re, shelve, time

def lambda_handler(event, context):
	n=filter(lambda x: x>='0' and x<='9', list(event['Records'][0]['s3']['object']['key']))[0]  #num mapper
	
	arxiu="Map"+str(n)
	bucket_name="mapreducerenricalvaro"

    #descarregar arxiu
	download_file(arxiu+".txt",bucket_name)
	text=open("/tmp/"+arxiu+".txt",'r').read()
	a=re.sub('[^ a-zA-Z0-9]',' ',text ).split()
	result={}

	time1=time.time()

	for paraula in a:
		if paraula not in result:
			result[paraula] = 1
		else:
			result[paraula] += 1

	print "TEMPS MAP"+str(n)+": "+str(time.time()-time1)

	#escriure a disc serialitzat
	arxiu_pujar='MapResult'+str(n)
	shelf=shelve.open("/tmp/"+arxiu_pujar+".shlf")
	shelf[arxiu_pujar]=result
	shelf.close()

	#carregar arxiu
	upload_file(arxiu_pujar+".shlf",bucket_name)
	return 'Mapper '+str(n)+' done!'
