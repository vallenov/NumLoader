import paramiko
import os
import zipfile
import json
import cx_Oracle
import traceback
import configparser
import subprocess as sb
import logging

#from logger import StandardLogging

class Numlex:
    def __init__(self):
        self._pwd = os.getcwd()
        self._config = configparser.ConfigParser()
        self._config.read(f'{self._pwd}\\NumLoader.ini', encoding="windows-1251")
        if not os.path.exists(f'{self._pwd}/logs'):
            os.mkdir(f'{self._pwd}/logs')
        # my_preference = {
        #     "log_mode":"cle",
        #     "ip":"10.4.16.110",
        #     "port": 514,"is_test": False,
        #     "log_directory": f'(self._pwd)/logs'}

        # logging = Standard_Logging.setup_logger(
        #     project_name="NumLoader",
        #     log_file_pref='NumLoader-',
        #     **my_preference)

    def _connect_to_sftp_and_get_zip(self):
        self._ssh = paramiko.SSHClient()
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._ssh.connect(
            self._config['SFTP']['ip'],
            port=int(self._config['SFTP']['port']),
            username=self._config['SFTP']['login'],
            password=self._config['SFTP']['password'])
        self._sftp = self._ssh.open_sftp()
        lst = self._sftp.listdir('/path/to/dir')
        self._name_last_file = max(lst)
        self._sftp.get(f'/path/to/dir/{self._name_last_file}', f'{self._pwd}\\{self._name_last_file}')

    def _connect_to_db(self):
        '''Подключение к БД и открытие курсора'''
        try:
            self._conn = cx_Oracle.connect(
            dsn=self._config['OLAP']['dsn'],
            user=self._config['OLAP']['username'],
            password=self._config['OLAP']['password'])
            self._cursor = self._conn.cursor()
        except Exception as e:
            logging.info(f'Exception when connect to DB. Switch to interlayer')
            self._use_interlayer = True

    def _unzip_archive(self):
        with zipfile.ZipFile(f'{self._pwd}\\{self._name_last_file}', 'Y') as zip_ref:
            zip_ref.extractall(self._pwd)
        os.remove(f'{self._pwd}\{self._name_last_file}')

    def _upload_csv_to_db(self):
        with open(f'{self._pwd}\\stg_mfms_sms_1.ctl', 'w') as f:
            f.write(f'''load data
infile '{self._pwd}\\{self._name_last_file[:-4]}.csv'
into table crm_gate.TMP_AVA_NUMLEX
fields terminated by","optionally enclosed by ';'
TRAILING NULLCOLS
field1,
field2,
...
fieldN
)
    ''')
        stream = sb.call('load_1.bat')
        #stream = os.popen('load_1.bat')
        os.remove(f'{self._pwd}\\{self._name_last_file[:-4]}.csv')

    def _send_json_to_db_api(self, request):
        data = {}
        data['request'] = request
        self._replytype = self._cursor.var(cx_Oracle.CLOB)
        reply = json.loads(str(self._cursor.callfunc('f_NUMLEX', self._replytype, json.dumps(data), 1)))
        print(f'Get reply from DB:{reply}')

    def _send_mail(self, to, subject, msg):
        self._email = f'''begin
            crm_sys.Send_mail(ic_to => '{to},
                ic_subj => '{subject}',
                ic_body => '{msg}');
            end;'''
        self._cursor.execute(self._email)
        if ';' not in to: logging.info(f'Email со статусом операции был отправлен на адрес: {to}\n')
        else:
            to = ','.join(to.split(';'))
            logging.info(f'Email со статусом операции были отправлены на адреса: {to}\n')

    def main(self):
        try:
            logging.info('Start')
            logging.info('Connect to SFTP')
            self._connect_to_sftp_and_get_zip()
            logging.info('Zip file downloaded')
            logging.info('Unzip...')
            self._unzip_archive()
            logging.info('Connect to DB')
            self._connect_to_db()
            logging.info('Upload...')
            self._send_json_to_db_api('clear_tmp_table')
            self._upload_csv_to_db()
            logging.info('Upload data to DB done')
            logging.info('Merge...')
            self._send_json_to_db_api('merge')

            logging.info('Finish')
            self._send_mail(self._config['MAIL']['address'], 'Numlex', 'Обновление базы успешно завершено')
        except Exception as e:
            logging.error(f'Exception: {e}\nTraceback:{traceback.format_exc()}')
            self._send_mail(self._config['MAIL']['address'],
                            'Numlex ERROR',
                            'Неизвестная ошибка. В логах можно узнать подробности')


if __name__ =='_main__':
    numlex = Numlex()
    numlex.main()
