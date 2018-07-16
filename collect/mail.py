"""Mail objects for reading a specified mail-message and returning the contents, including attachments."""
import imapclient, email, os, subprocess, re, shutil
from os.path import join, split, splitext, exists

class Email(object):
    def __init__(self, host):
        self._host = host

root_mail = r"P:\data\source\kpn\swol_marketing\email"

def example_dennis():
    host = 'imap.gmail.com'
    server = imapclient.IMAPClient(host, use_uid=True, ssl=True)
    server.login('gerard.lutterop@bizservices.nl', 'Mu7t1p1aF1at')
    server.select_folder('INBOX')
    message_ids = []
    for crit in ('FROM "dennis.hoogstad@kpn.com"', 'FROM "gerard.j.lutterop@kpn.com"', 'FROM "dennisseerden@kpn.com"'):
        for msg_id in server.search([crit]):
            if not exists(join(root_mail, str(msg_id))):
                message_ids.append(msg_id)
    #msgs = server.fetch(message_ids, ['INTERNALDATE'])
    msgs = server.fetch(message_ids, ['INTERNALDATE', 'RFC822'])
    for k, v in msgs.items():
        e = email.message_from_string(v['RFC822'])
        for part in e.walk():
            if part.get_content_type() == 'application/octet-stream':
                # Save the contents:
                path = join(root_mail, str(k))
                if not exists(path):
                    os.makedirs(path)
                filename = part.get_filename()
                open(join(path, filename), 'wb').write(part.get_payload(decode=1))
                if splitext(filename)[-1].lower() in ('.zip', '.rar'):
                    # Get the contents by calling the 7z-program:
                    for pwd in ('q77bLk', None):
                        args = [r"C:\Program Files\7-Zip\7z.exe", 'e', '-o' + path, '-y', join(path, filename)]
                        if pwd:
                            args.insert(2, '-p{}'.format(pwd))
                        try:
                            result = subprocess.check_output(args)
                            if not 'Everything is Ok' in result:
                                print result
                                raise OSError(result)
                            else:
                                print 'Done: {}'.format(filename)
                                break
                        except Exception as e:
                            print e

def example_odin():
    host = 'imap.gmail.com'
    server = imapclient.IMAPClient(host, use_uid=True, ssl=True)
    server.login('gerard.lutterop@bizservices.nl', 'Mu7t1p1aF1at')
    server.select_folder('INBOX')
    for crit in 'FROM "noreply-softwareonline@kpn.com"', 'FROM "cloud@kpn.com"':
        message_ids = server.search([crit])
        message_ids = [msg_id for msg_id in message_ids if not exists(join(root_mail, str(msg_id)))]
        #msgs = server.fetch(message_ids, ['INTERNALDATE'])
        msgs = server.fetch(message_ids, ['INTERNALDATE', 'RFC822'])
        for k, v in msgs.items():
            e = email.message_from_string(v['RFC822'])
            print e.get('Received')
            for part in e.walk():
                # Save the contents:
                path = join(root_mail, str(k))
                if not exists(path):
                    os.makedirs(path)
                if part.get_filename():
                    filename = part.get_filename()
                    open(join(path, filename), 'wb').write(part.get_payload(decode=1))
    for dirpath, dirnames, filenames in os.walk(root_mail):
        path_dest = r"P:\data\source\kpn\swol_marketing\odin_rapportage\flat"
        for filename in filenames:
            if re.match('rapportage_\d+\-\d+\-\d+\.csv', filename):
                if not os.path.exists(join(path_dest, filename)):
                    shutil.copy2(join(dirpath, filename), path_dest)

def example_ip_clean():
    host = 'imap.gmail.com'
    server = imapclient.IMAPClient(host, use_uid=True, ssl=True)
    server.login('gerard.lutterop@bizservices.nl', 'Mu7t1p1aF1at')
    server.select_folder('INBOX')
    for crit in 'FROM "ip_cleaner@kpn.com"',:
        message_ids = server.search([crit])
        message_ids = [msg_id for msg_id in message_ids if not exists(join(root_mail, str(msg_id)))]
        #msgs = server.fetch(message_ids, ['INTERNALDATE'])
        msgs = server.fetch(message_ids, ['INTERNALDATE', 'RFC822'])
        for k, v in msgs.items():
            e = email.message_from_string(v['RFC822'])
            print e.get('Received')
            for part in e.walk():
                # Save the contents:
                path = join(root_mail, str(k))
                if not exists(path):
                    os.makedirs(path)
                if part.get_filename():
                    filename = part.get_filename()
                    open(join(path, filename), 'wb').write(part.get_payload(decode=1))
    for dirpath, dirnames, filenames in os.walk(root_mail):
        path_dest = r"P:\data\source\kpn\ip_cleaner\reports"
        for filename in filenames:
            if re.match('ip_cleaner_.*_\d+\-\d+\-\d+\.csv', filename):
                if not os.path.exists(join(path_dest, filename)):
                    shutil.copy2(join(dirpath, filename), path_dest)

def example_ots():
    host = 'imap.gmail.com'
    server = imapclient.IMAPClient(host, use_uid=True, ssl=True)
    server.login('gerard.lutterop@bizservices.nl', 'Mu7t1p1aF1at')
    server.select_folder('INBOX')
    message_ids = []
    for crit in ('BODY "8918L12237"',):
        for msg_id in server.search([crit]):
            if not exists(join(root_mail, str(msg_id))):
                message_ids.append(msg_id)
    #msgs = server.fetch(message_ids, ['INTERNALDATE'])
    msgs = server.fetch(message_ids, ['INTERNALDATE', 'RFC822'])
    for k, v in msgs.items():
        e = email.message_from_string(v['RFC822'])
        for part in e.walk():
            if part.get_content_type() in ('application/octet-stream', 'application/x-zip-compressed'):
                # Save the contents:
                path = join(root_mail, str(k))
                if not exists(path):
                    os.makedirs(path)
                filename = part.get_filename()
                print filename
                if splitext(filename)[-1].lower() in ('.zip',) and 'b8918' in filename.lower():
                    # Get the contents by calling the 7z-program:
                    open(join(path, filename), 'wb').write(part.get_payload(decode=1))
                    for pwd in ('paardenstaart', None):
                        args = [r"C:\Program Files\7-Zip\7z.exe", 'e', '-o' + path, '-y', join(path, filename)]
                        if pwd:
                            args.insert(2, '-p{}'.format(pwd))
                        try:
                            result = subprocess.check_output(args)
                            if not 'Everything is Ok' in result:
                                print result
                                raise OSError(result)
                            else:
                                print 'Done: {}'.format(filename)
                                break
                        except Exception as e:
                            print e

if __name__ == '__main__':
    example_ip_clean()
    example_odin()
    #example_dennis()
    example_ots()