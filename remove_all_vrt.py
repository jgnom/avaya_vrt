import paramiko
import time
import re
from sys import argv


host = argv[1]
port = argv[2]
un = argv[3]
pwd = argv[4]
pin = argv[5]
term = 'ossi'
timeout = 0.5

field_name_vrt = '8003ff00'


def rem_all_vrt():
    ssh = connect(host, port, un, pwd, term, pin)
    time.sleep(timeout)
    ssh.send(f"clist vrt\nt\n")
    time.sleep(timeout)
    src = ssh.recv(60000).decode('utf-8')
    slice = src.partition("4654ff00\n")[2]
    num_vrt = re.findall(r'd\d{1,3}\t', slice)
    nm_vrt = []
    for i in num_vrt:
        t = int(re.sub(r'\D', '', i))
        nm_vrt.append(t)
    cnt_vrt = len(nm_vrt)

    cmd_for_rem = ''
    for i in nm_vrt:
        cmd = f"crem vrt {i}\nt\n"
        cmd_for_rem = cmd_for_rem + cmd

    cmd_for_rem = cmd_for_rem+"\n"
    ssh.send(cmd_for_rem)
    time.sleep(10)
    print("All vrt tables has been removed!!!")
    ssh.close()


def connect(host, port, username, password, term, pin=None):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=username, password=password, port=port, look_for_keys=False, allow_agent=False)
    ssh = client.invoke_shell()
    ssh.settimeout(timeout)
    time.sleep(timeout)
    ssh.send(f"{pin}\n")
    time.sleep(timeout)
    ssh.send(f"{term}\n")
    time.sleep(timeout)
    return ssh


if __name__ == '__main__':
    rem_all_vrt()
