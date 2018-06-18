import boto3,botocore,os,sys,time,shelve,time
from boto3.session import Session


def getCredentials():
  with open(sys.argv[4]) as f:
   credentials=[x.strip().split('=') for x in f.readlines()]
  global ACCESS_KEY
  global SECRET_KEY
  global bucket_name
  ACCESS_KEY=credentials[0][1]
  SECRET_KEY=credentials[1][1]
  bucket_name=credentials[2][1]

def separate(input_file,num_mappers,folder):

	os.system('mkdir -p '+folder)
	linies=longitud_fitxer(input_file)
	lin_map=(linies/num_mappers)
	file=open(input_file,'r')
	m=0
	while(m<num_mappers):
		mapperFile=open(folder+'/Mapper'+str(m)+'.txt','w')
		print('Creat arxiu: Mapper'+str(m))
		j=0
		while(j<lin_map):
			linia=file.readline()
			mapperFile.write(linia)
			j+=1
		mapperFile.close()
		m+=1

	mapperFile=open(folder+'/Mapper'+str(m-1)+'.txt','a')
	j=0
	while True:
		linia=file.readline()
		if not linia: break
		mapperFile.write(linia)

	mapperFile.close()

	file.close()

def longitud_fitxer(file):
    with open(file) as f:
        for i, l in enumerate(f):
	    pass
    return i+1

def upload_file_master(n,folder):
    try:
        client = boto3.client('s3',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY)
	print ('Pujant arxius al bucket...')

	os.chdir(folder)
	for i in range(0,n):
		mapperFile='Mapper'+str(i)+'.txt'
		client.upload_file(mapperFile,bucket_name,mapperFile)
		print ('Arxiu pujat al bucket: '+mapperFile)

    except Exception,e:
        print str(e)
        print "error"

def upload_file_reducer(n):
    try:
        client = boto3.client('s3',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY)

	reducerFile=str(n)+".red"
	f= open(reducerFile,"w+")
	f.close()
	client.upload_file(reducerFile,bucket_name,reducerFile)

    except Exception,e:
	print str(e)
	print "error"

def wait_mappers(n):
	session = Session(aws_access_key_id=ACCESS_KEY,
		          aws_secret_access_key=SECRET_KEY)
	s3 = session.resource('s3')
	your_bucket = s3.Bucket(bucket_name)

	while (sum(1 for s3_file in your_bucket.objects.filter(Prefix='MapResult'))!=n):
		print 'Esperant calcul mappers...'
	upload_file_reducer(n)

def wait_reducer(time1):
	session = Session(aws_access_key_id=ACCESS_KEY,
		          aws_secret_access_key=SECRET_KEY)
	s3 = session.resource('s3')
	your_bucket = s3.Bucket(bucket_name)

	while (sum(1 for s3_file in your_bucket.objects.filter(Prefix='Reduce'))!=1):
		print 'Esperant calcul reducer...'

	receive_result(time1)

def receive_result(time1):
    try:
        client = boto3.client('s3',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY)

	reducerFile='Reduce.shlf'
	key="Reduce"
    	totalTime=(time.time()-time1)
	print 'Obtenint resultat final...'
	client.download_file(bucket_name, reducerFile, reducerFile)

	print shelve.open(reducerFile)[key]
    	print str(totalTime)
    except Exception,e:
        print str(e)
        print "error"

def clean_bucket():
	print "from delete: " + bucket_name
	session = Session(aws_access_key_id=ACCESS_KEY,
		          aws_secret_access_key=SECRET_KEY)
	s3 = session.resource('s3')
	your_bucket = s3.Bucket(bucket_name)

	for s3_file in your_bucket.objects.all():
		s3.Object(bucket_name,s3_file.key).delete()
	print 'Arxius del bucket esborrats'


#num_map, ini_file, folder, arxius_credentials
if __name__ == "__main__":
    num_mappers=int(sys.argv[1])
    ini_file=sys.argv[2]
    folder=sys.argv[3]

    getCredentials()

    clean_bucket()
    separate(ini_file,num_mappers,folder)
    upload_file_master(num_mappers,folder)
    time1=time.time()
    wait_mappers(num_mappers)
    wait_reducer(time1)
