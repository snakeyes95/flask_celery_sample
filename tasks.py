from flask import Flask,request
from celery import Celery
from time import sleep
import uuid
import requests
from flask_restful import Resource,Api,reqparse
import subprocess 
import time
from flask_sqlalchemy import SQLAlchemy
import datetime
import os
from flasgger import Swagger

#redis://localhost:6379/0
#sqlite:///data.db
db=SQLAlchemy()
app=Celery('tasks',broker='pyamqp://guest@localhost//')

def create_app():
    flask_app=Flask(__name__)
    flask_app.config['SQLALCHEMY_DATABASE_URI']='mysql+pymysql://root:root@localhost/testdb'
    flask_app.config['SQLALCHEMY_TRACK_MODIFICATIONS']=False
    flask_app.config['PROPAGATE_EXCEPTIONS']=True
    db.init_app(flask_app) 
    return flask_app



class BackupModel(db.Model):
    __tablename__="backup"

    id=db.Column(db.Integer(),primary_key=True)
    srclang=db.Column(db.String(10))
    trgtlang=db.Column(db.String(10))
    srcseg=db.Column(db.UnicodeText())
    trgtseg=db.Column(db.UnicodeText())
    taskid=db.Column(db.Text())
    sentid=db.Column(db.Integer())
    callbackurl=db.Column(db.Text())
    timetaken=db.Column(db.Float(precision=2))

    def __init__(self,srclang,trgtlang,srcseg,trgtseg,taskid,sentid,callbackurl,timetaken):
        self.srclang=srclang
        self.trgtlang=trgtlang
        self.srcseg=srcseg
        self.trgtseg=trgtseg
        self.taskid=taskid
        self.sentid=sentid
        self.callbackurl=callbackurl
        self.timetaken=timetaken

    def save_to_db(self):
        app=create_app()
        with app.app_context():
            db.session.add(self)
            db.session.commit()

    def json(self):
        return {"srclang":self.srclang,
                "trgtlang":self.trgtlang,
                "srcseg":self.srcseg,
                "trgtseg":self.trgtseg,
                "sentid":self.sentid,
                "callbackurl":self.callbackurl,
                "timetaken":self.timetaken
                }
    
    @classmethod
    def filter_by_taskid(self,taskid):
        return cls.query.filter_by(taskid=taskid).first()

    @classmethod
    def filter_by_sentid(self,sentid):
        return cls.query.filter_by(sentid=sentid).first()
   



#@flask_app.before_first_request
#def create_tables():
 #   db.create_all()


@app.task
def get_translation_single(srclangcode,trgtlangcode,srctext,id,sentid,callbackurl):
    start_time=time.time()
    with open(id+'input_infer.txt','w',encoding='utf-8')as fobj:
        fobj.write(srctext)
    with open(id+'output_infer.txt','w',encoding='utf-8') as fobj:
        pass       
    subprocess.run('python ./OpenNMT-py/translate.py -model ./model/transformer_baseline_model/release_model_1.pt -src ./'+id+'input_infer.txt -output ./'+id+'output_infer.txt -replace_unk',check=True)
    with open(id+'output_infer.txt','r',encoding='utf-8') as fobj:
        trgtseg=fobj.readline()
    stop_time=time.time()
    timetaken = stop_time - start_time
    backup=BackupModel(srclangcode,trgtlangcode,srctext,trgtseg,id,sentid,callbackurl,timetaken)
    backup.save_to_db()

    # return {"message":f"error occured while trying to insert into backup {timetaken} {srctext} {trgtseg}"}
    if os.path.exists(id+'output_infer.txt'):
        os.remove(id+'output_infer.txt')
    else:
        print(f'{id} file not found')
    if os.path.exists(id+'input_infer.txt'):
        os.remove(id+'input_infer.txt')
    else:
        print(f'{id} file not found')

    return 'get Translation single called'+str(id)+str(srctext)







class GetSingleTranslation(Resource):
    def get(self):
        return 'Hello world'

    def post(self):
        request_parser = reqparse.RequestParser(bundle_errors=True)
        request_parser.add_argument('SrcLangCode',type=str,required=True,help='Enter Source language code',location='json')
        request_parser.add_argument('TrgtLangCode',type=str,required=True,help='Enter Target language code',location='json')
        request_parser.add_argument('SrcSent', type = str, required = True,help = 'No Source text provided', location = 'json')
        request_parser.add_argument('SentId', type = int, required = True,help = 'Please specify SentId', location = 'json')
        request_parser.add_argument('CallbackURL', type = str, required = True,help = 'Please specify CallbackURL to recieve response', location = 'json')

        

        
        args=request_parser.parse_args()
        srclangcode=args['SrcLangCode']
        trgtlangcode=args['TrgtLangCode']
        sentid=args['SentId']
        srcsent=args['SrcSent']
        callback=args['CallbackURL']

        if len(srcsent)==0:
            return { 'message': "Empty Source string passed" }, 404   
        if len(srclangcode)==0:
            return { 'message': "Empty SrcLangCode passed" }, 404   
        if len(trgtlangcode)==0:
            return { 'message': "Empty TrgtLangCode passed" }, 404   
        if sentid == 0:
            return { 'message': "Invalid SentId passed" }, 404   
        if len(callback)==0:
            return { 'message': "Empty CallbackURL string passed" }, 404      
        
        id=uuid.uuid1()
        #req_data=request.get_json()
        
        get_translation_single.delay(srclangcode,trgtlangcode,str(srcsent),id,sentid,callback)
        return {"status":"Request accpeted",
                "TaskID":str(id),
                "data":args},202

#@flask_app.route('/gettranslationsingle',methods=['POST'])
#def hello():
 #   id=uuid.uuid1()
  #  contents='data'
   # get_translation_single.delay(id)
    #return 'Async request accepted!' , 302
class GetSingleSyncTrans(Resource):
    def post(self):
        """
       Get Translation Sync
       This endpoint provides result in a sync manner
       ---
       consumes:
         - application/json
       parameters:
         - in: body
           name: SrcLangCode
           type: string
           required: true
         - in: json
           name: SrcLangCode
           type: string
           required: true
         - in: json
           name: TrgtLangCode
           type: string
           required: true
         - in: json
           name: SrcSent
           type: string
           required: true
         - in: json
           name: SentId
           type: string
           required: true
         - in: json
           name: CallbackURL
           type: string
           required: true
       responses:
         200:
           description: Translation completed sucessfully
           schema:
             id: Result
             properties:
               status:
                 type: string
                 description: status of the task
               TaskID:
                 type: string
                 description: internal taskid assigned
               data:
                 type: object
                 description: request parameters recieved
               result:
                 type: object
                 description: target segment and time taken 
                 
        """
        request_parser = reqparse.RequestParser(bundle_errors=True)
        request_parser.add_argument('SrcLangCode',type=str,required=True,help='Enter Source language code',location='json')
        request_parser.add_argument('TrgtLangCode',type=str,required=True,help='Enter Target language code',location='json')
        request_parser.add_argument('SrcSent', type = str, required = True,help = 'No Source text provided', location = 'json')
        request_parser.add_argument('SentId', type = int, required = True,help = 'Please specify SentId', location = 'json')
        request_parser.add_argument('CallbackURL', type = str, required = True,help = 'Please specify CallbackURL to recieve response', location = 'json')

        

        
        args=request_parser.parse_args()
        srclangcode=args['SrcLangCode']
        trgtlangcode=args['TrgtLangCode']
        sentid=args['SentId']
        srcsent=args['SrcSent']
        callback=args['CallbackURL']

        if len(srcsent)==0:
            return { 'message': "Empty Source string passed" }, 404   
        if len(srclangcode)==0:
            return { 'message': "Empty SrcLangCode passed" }, 404   
        if len(trgtlangcode)==0:
            return { 'message': "Empty TrgtLangCode passed" }, 404   
        if sentid == 0:
            return { 'message': "Invalid SentId passed" }, 404   
        if len(callback)==0:
            return { 'message': "Empty CallbackURL string passed" }, 404      
        
        id=uuid.uuid1()
        #req_data=request.get_json()
        start_time=time.time()
        with open(str(id)+'input_infer.txt','w',encoding='utf-8')as fobj:
            fobj.write(srcsent)
        with open(str(id)+'output_infer.txt','w',encoding='utf-8') as fobj:
            pass       
        subprocess.run('python ./OpenNMT-py/translate.py -model ./model/transformer_baseline_model/release_model_1.pt -src ./'+str(id)+'input_infer.txt -output ./'+str(id)+'output_infer.txt -replace_unk',check=True)
        with open(str(id)+'output_infer.txt','r',encoding='utf-8') as fobj:
            trgtseg=fobj.readline()
        stop_time=time.time()
        timetaken = stop_time - start_time

        if os.path.exists(str(id)+'output_infer.txt'):
            os.remove(str(id)+'output_infer.txt')
        else:
            print(f'{str(id)} file not found')
        if os.path.exists(str(id)+'input_infer.txt'):
            os.remove(str(id)+'input_infer.txt')
        else:
            print(f'{str(id)} file not found')
        
        result={ "targetSegment":trgtseg,
            "timetaken":timetaken
                   }

        return {"status":"Request completed!",
                "TaskID":str(id),
                "data":args,
                "result":result},200

if __name__ == "__main__":
    flask_app=create_app()
    with flask_app.app_context():
        db.create_all()
    api=Api(flask_app) 
    swagger = Swagger(flask_app)
    api.add_resource(GetSingleTranslation,'/getsingle')
    api.add_resource(GetSingleSyncTrans,'/getsinglesync')
    flask_app.run(port=5000)