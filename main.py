import paramiko
import time
import re
import pandas as pd
from sys import argv


host = argv[1]
port = argv[2]
un = argv[3]
pwd = argv[4]
pin = argv[5]
file = argv[6]
term = 'ossi'
timeout = 0.5
field_name_vrt = '8003ff00'
timeout_record_to_vrt = 1
timeout_for_create_vrt = 20


def main(filename):
    starttime = time.time()
    # get existing resords from vrt tables
    vrt_all = get_exist_records()
    # create dictionary from recieved data
    exist_dct = create_dict_for_exist_vrt(vrt_all)
    # get phone_numbers from csv file.
    ls_new_num = import_xls(filename)
    # get uniques phone_numbers from both place and compare data
    uni_record = compare_num(exist_dct, ls_new_num)
    # create additionaly tables vrt if neccesary
    result = crt_nec_vrt(vrt_all, uni_record)
    if result[0] == -1:
        print("exceeded max count records!!!")
        print(f"Total number {result[1]} cell available to record!!!")
    # get new dictionary with number busy cells in each table
    vrt_all_new = get_exist_records()
    # get list standarts id for vrt
    ls_std_fields = get_std_fields()
    # get new full dictionary with and data vrt.
    vrt_all_exist_new = create_new_dict_for_exist_vrt(vrt_all_new)
    # get command string for sending in to Communication Manager for record in vrt
    cmd_ls_for_record = cmd_for_record(uni_record, vrt_all_exist_new)
    # record in Communication Manager phone_numbers in vrt tables
    for i in cmd_ls_for_record:
        put_to_cm(i)
    end_time = time.time() - starttime
    print(f"Time for execution ... {end_time} sec")


def put_to_cm(cm_string):
    ssh = connect(host, port, un, pwd, term, pin)
    time.sleep(timeout)
    ssh.send(cm_string)
    time.sleep(2)
    print(f"Recorded in CM {len(cm_string)} bytes")
    ssh.close()


def gen_num(ls):
    while ls:
        yield ls.pop()


def get_std_fields():
    ssh = connect(host, port, un, pwd, term, pin)
    time.sleep(timeout)
    ssh.send("cdispl vrt 1\nt\n")
    time.sleep(timeout)
    msg = ssh.recv(6000000).decode("utf-8")
    ls_f = re.findall(r"4653ff..", msg)
    ssh.close()
    return ls_f


def cmd_for_record(uni_record, vrt_all_exist_new):
    cm_trash = ''
    ls_cm_cmd = []
    for k in vrt_all_exist_new.keys():

        dc = vrt_all_exist_new[k]
        str_cmd = ''
        ch_vrt = f"cch vrt {k}\n"
        te = f"t\n"
        for key, value in dc.items():
            try:
                if value == '':
                    nm = next(gen_num(uni_record))
                    str_tmp = f"f{key}\n" + f"d{nm}\n"
                    str_cmd = str_cmd + str_tmp
            except StopIteration:
                pass
        str_cmd = ch_vrt + str_cmd + te
        ls_cm_cmd.append(str_cmd)
    count_cmd = 1
    new_ls_cmd = []
    buffer = ''
    for i in ls_cm_cmd:
        if count_cmd < 10:
            if len(i) > 20:
                buffer += i
                count_cmd += 1
            else:
                cm_trash += i
        else:
            buffer += i
            new_ls_cmd.append(buffer)
            buffer = ''
            count_cmd = 1
    new_ls_cmd.append(buffer)
    return new_ls_cmd


def crt_nec_vrt(dct_exist_vrt_all, uni_record):
    ssh = connect(host, port, un, pwd, term, pin)
    time.sleep(timeout)
    cnt_uni_record = len(uni_record)
    exist_vrt_record = 0
    for i in dct_exist_vrt_all.values():
        exist_vrt_record += i
    print("Total existing records in vrt tables - ", exist_vrt_record)
    print("Total new phone_numbers for record in VRT table - ", cnt_uni_record)
    # computation free cell in vrt tables
    free_cell = 0
    for key, value in dct_exist_vrt_all.items():
        cnt_free = 100 - value
        free_cell += cnt_free
    print("Total free cells for record - ", free_cell)
    tmp = 0
    new_record_possible = 99900 - exist_vrt_record
    if free_cell >= cnt_uni_record:
        tmp = 0
    elif free_cell < cnt_uni_record:
        tmp = cnt_uni_record - free_cell
        if tmp <= 100:
            tmp = 1
        elif cnt_uni_record + exist_vrt_record > 99900:
            tmp = -1
        elif tmp % 100 == 0:
            tmp = tmp // 100
        else:
            tmp = tmp // 100 + 1

    print(f"Necessary numbers additionals vrt tables {tmp}")
    cmd_for_add_vrt = ''
    ls_cmd_for_add_vrt = []
    buf = 1
    for i in range(1, tmp+1):
        if buf < 30:
            tmp_cmd = f"cadd vrt next\nt\n"
            cmd_for_add_vrt += tmp_cmd
            buf += 1
        else:
            tmp_cmd = f"cadd vrt next\nt\n"
            cmd_for_add_vrt += tmp_cmd
            ls_cmd_for_add_vrt.append(cmd_for_add_vrt)
            buf = 1
            cmd_for_add_vrt = ''
    ls_cmd_for_add_vrt.append(cmd_for_add_vrt)

    count = 1
    for i in ls_cmd_for_add_vrt:
        time.sleep(1)
        ssh.send(i)
        print(f"{count} part of {len(ls_cmd_for_add_vrt)} create tables vrt")
        count += 1
    time.sleep(1)
    print(f"Created vrt tables - {tmp}")
    result = []
    result.append(tmp)
    result.append(new_record_possible)
    ssh.close()
    return result


def compare_num(exist_num_dict, new_num_list):
    num_for_record = []
    exist_ls_num = []
    for key, value in exist_num_dict.items():
        for v in value.values():
            if v != '':
                exist_ls_num.append(v)
    set_new_num = set(new_num_list)
    set_exist_num = set(exist_ls_num)
    for i in set_new_num:
        if i not in set_exist_num:
            num_for_record.append(i)
    return num_for_record


def import_xls(name):
    pd.set_option('display.float_format', '{:.0f}'.format)
    file = pd.read_csv(name)
    df = pd.DataFrame(file)
    ls_numb = df['phone numbers'].tolist()
    ls_numb = list(map(str, ls_numb))
    ls_num = []
    for i in ls_numb:
        y = re.sub('\.0', '', i)
        if y == 'nan':
            continue
        ls_num.append(y)
    return ls_num


def connect(host, port, username, password, term, pin=None):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=username, password=password, port=port, look_for_keys=False, allow_agent=False)
    ssh = client.invoke_shell()
    time.sleep(timeout)
    ssh.send(f"{pin}\n")
    time.sleep(timeout)
    ssh.send(f"{term}\n")
    time.sleep(timeout)
    return ssh


def get_exist_records():
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
    cnt_num_vrt = re.findall(r'\d{1,3}\n', slice)
    cnt_vrt = []
    cnt_num_in_vrt = 0
    for i in cnt_num_vrt:
        t = int(re.sub(r'\n', '', i))
        cnt_vrt.append(t)
        cnt_num_in_vrt += t

    dict_vrt = dict(zip(nm_vrt, cnt_vrt))

    print("Total existing records in vrt tables ", cnt_num_in_vrt)
    ssh.close()
    return dict_vrt


def create_new_dict_for_exist_vrt(dct):
    ssh = connect(host, port, un, pwd, term, pin)
    time.sleep(timeout)
    cmd_string = ''
    for key, value in dct.items():
        tmp = f"cdispl vrt {key}\nt\n"
        cmd_string = cmd_string + tmp
    ssh.send(cmd_string)
    time.sleep(timeout)
    src = ssh.recv(6000000).decode('utf-8')
    all_str = src.split('cdispl vrt ')
    del all_str[0]
    ls_1 = []
    for i in all_str:
        tmp = i.split('\n')
        ls_1.append(tmp)
    vrt_numbs = []
    for i in ls_1:
        vrt_numbs.append(int(i[0]))
    ls_fi = []
    ls_da = []
    ls_num_vrt = []
    for i in all_str:
        ls_f = re.findall(r"4653ff..", i)
        ls_d = re.split(r'\nd\d{,3}\t.{,15}\t[y,n]\t', i)
        num_vrt = re.findall(r'^\d{1,3}', ls_d[0])
        ls_d = re.sub(r'\n', '\t', ls_d[1])
        ls_d = re.sub('[a-z]', '', ls_d).split('\t')
        del ls_d[100:102]
        ls_fi.append(ls_f)
        ls_da.append(ls_d)
        ls_num_vrt.append(num_vrt[0])
    for i, item in enumerate(ls_num_vrt):
        ls_num_vrt[i] = int(item)
    dict_vrt_f_d = {}
    tmp_dct = {}
    for i in ls_num_vrt:
        tmp_dct[i] = ''
    cnt = 0
    while cnt < len(tmp_dct):
        for key in tmp_dct.keys():
            ls_f = ls_fi[cnt]
            ls_d = ls_da[cnt]
            cnt += 1
            dct_temp = dict(zip(ls_f, ls_d))
            for k, v in dct_temp.items():
                dict_vrt_f_d.setdefault(key, {}).update({k: v})
    ssh.close()
    return dict_vrt_f_d


def create_dict_for_exist_vrt(dct):
    ssh = connect(host, port, un, pwd, term, pin)
    time.sleep(timeout)
    cmd_string = ''
    for key, value in dct.items():
        if value != 0:
            tmp = f"cdispl vrt {key}\nt\n"
            cmd_string = cmd_string + tmp
    ssh.send(cmd_string)
    time.sleep(timeout)
    src = ssh.recv(6000000).decode('utf-8')
    all_str = src.split('cdispl vrt ')
    del all_str[0]
    ls_1 = []
    for i in all_str:
        tmp = i.split('\n')
        ls_1.append(tmp)
    vrt_numbs = []
    for i in ls_1:
        vrt_numbs.append(int(i[0]))
    ls_fi = []
    ls_da = []
    ls_num_vrt = []
    for i in all_str:
        ls_f = re.findall(r"4653ff..", i)
        ls_d = re.split(r'\nd\d{,3}\t.{,15}\t[y,n]\t', i)
        num_vrt = re.findall(r'^\d{1,3}', ls_d[0])
        ls_d = re.sub(r'\n', '\t', ls_d[1])
        ls_d = re.sub('[a-z]', '', ls_d).split('\t')
        del ls_d[100:102]
        ls_fi.append(ls_f)
        ls_da.append(ls_d)
        ls_num_vrt.append(num_vrt[0])
    for i, item in enumerate(ls_num_vrt):
        ls_num_vrt[i] = int(item)
    dict_vrt_f_d = {}
    tmp_dct = {}
    for i in ls_num_vrt:
        tmp_dct[i] = ''
    cnt = 0
    while cnt < len(tmp_dct):
        for key in tmp_dct.keys():
            ls_f = ls_fi[cnt]
            ls_d = ls_da[cnt]
            cnt += 1
            dct_temp = dict(zip(ls_f, ls_d))
            for k, v in dct_temp.items():
                dict_vrt_f_d.setdefault(key, {}).update({k: v})
    ssh.close()
    return dict_vrt_f_d


if __name__ == '__main__':
    main(file)
