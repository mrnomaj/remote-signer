
import logging
import sqlite3
import sys
from os import environ

class SystemDB:
    MIN_ESCROW_TIME = 0
    MAX_ESCROW_TIME = 0
    MAX_THRESHOLD = 0
    conn = None
    filename = ''

    def __init__(self, filename, min_escrow_time, max_escrow_time):
        self.filename = filename
        self.MIN_ESCROW_TIME = min_escrow_time
        self.MAX_ESCROW_TIME = max_escrow_time
        self.MAX_THRESHOLD = 100000 * 1000000 # 100k tez
        self.provision_database()

    def connect(self):
        self.conn = sqlite3.connect(self.filename)
        #self.conn = conn.conn()

    def disconnect(self):
        try:
            with self.conn:
                #self.conn.commit()
                self.conn.close()
                self.conn = None
        except:
            a = 1 #noop

    def provision_database(self):
        self.connect()
        # self.conn.row_factory = dict_factory
        with self.conn as conn:
            conn.execute('CREATE TABLE IF NOT EXISTS operations (id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT, amt INTEGER, src TEXT, dest TEXT, fee INTEGER, createdTime DATETIME)')
        self.disconnect()
        self.op_cleanup() 

    def op_select(self, id=-1):
        self.op_cleanup(True)
        returnLst = []
        opLst = []
        names = []
        try:
            if id < 0:
                # select all
                with self.conn:
                    opLst = self.conn.execute('SELECT * FROM operations ORDER BY id ASC')
                    names = [d[0] for d in opLst.description]
                    opLst = opLst.fetchall()
                    
            else:
                with self.conn:
                    opLst = self.conn.execute('SELECT * FROM operations WHERE id=? ORDER BY id ASC', (id,))
                    names = [d[0] for d in opLst.description]
                    opLst = opLst.fetchall()
            for op in opLst:
                returnLst.append(dict(zip(names, op)))
            return returnLst
        except:
            logging.info('Error in operation select: {}'.format(sys.exc_info()))
            return {'error': 'check the logs for details, most likely id was not an int'}
        finally:
            self.disconnect()

    def op_internal_select(self, params):
        self.op_cleanup(True)
        returnValue = -1
        try:
            with self.conn:
                opLst = self.conn.execute('SELECT * FROM operations WHERE type=? AND amt=? AND src=? AND dest=? AND fee=? AND strftime("%s",DATETIME("now", "localtime")) \
                    - strftime("%s",createdTime) >= ? AND strftime("%s",DATETIME("now","localtime")) - strftime("%s",createdTime) < ? ORDER BY id ASC', \
                    (params['type'], params['amt'], params['src'], params['dest'], params['fee'], self.MIN_ESCROW_TIME, self.MAX_ESCROW_TIME))
                names = [d[0] for d in opLst.description]
                opLst = opLst.fetchall()
                if len(opLst) > 0:
                    returnValue = dict(zip(names, opLst[0]))['id']
        except:
            logging.info('Error in operation insert: {}'.format(sys.exc_info()))
        finally:
            self.disconnect()
            return returnValue

    def op_insert(self, params):
        self.op_cleanup(True)
        returnValue = {}
        try:
            if params['amt'] <= self.MAX_THRESHOLD:
                with self.conn:
                    returnValue['rows'] = self.conn.execute('INSERT INTO operations (type,amt,src,dest,fee,createdTime) \
                        VALUES (?,?,?,?,?,DATETIME("now", "localtime"))', (params['type'], params['amt'], params['src'], params['dest'], params['fee'])).rowcount
                if returnValue['rows'] > 0:
                    logging.info('Inserted new operation')
            else:
                returnValue['error'] = 'amount exceeded max threshold of {} tez'.format(self.MAX_THRESHOLD / 1000000)
        except:
            logging.info('Error in operation insert: {}'.format(sys.exc_info()))
            returnValue = {'error': 'check the logs for details, most likely params was an incorrect format'}
        finally:
            self.disconnect()
            return returnValue

    def op_delete(self, id):
        returnValue = {}
        try:
            with self.conn:
                self.conn.execute('DELETE FROM operations WHERE id=?', id)
            return {}
        except:
            logging.info('Error in operation delete: {}'.format(sys.exc_info()))
            returnValue = {'error': 'check the logs for details, maybe id was a valid primary key value?'}
        finally:
            self.disconnect()
            return returnValue

    def op_cleanup(self, leaveOpen=False):
        self.connect()
        returnValue = {}
        try:
            with self.conn:
                returnValue['deleted'] = self.conn.execute('DELETE FROM operations WHERE strftime("%s",DATETIME("now", "localtime")) \
                    - strftime("%s",createdTime) < 0 OR strftime("%s",DATETIME("now","localtime")) - strftime("%s",createdTime) > ?', (self.MAX_ESCROW_TIME,)).rowcount
            if returnValue['deleted'] > 0:
                logging.info('Cleaned up {} rows'.format(returnValue['deleted']))
        except:
            logging.info('Error in operation cleanup: {}'.format(sys.exc_info()))
            returnValue = {'error': 'check the logs for details, unknown error'}
        finally:
            if leaveOpen is not True:
                self.disconnect()
            return returnValue
