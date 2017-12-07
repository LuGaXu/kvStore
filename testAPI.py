import json
import random
import requests
import argparse

def test_put(pov_num, hosts, ports, data_num):
    for i in range(0,data_num):
        url = random_ip(pov_num, hosts, ports)+'/process'
        print url
        data = get_data(i)
        put(url, data)


def test_branch_put(pov_num, hosts, ports, data_num):
    url = random_ip(pov_num, hosts, ports) + '/batchProcess'
    data = {}
    for i in range(0, data_num):
        key = str(i)
        data[key] = {'column'+str(i): 'value'+str(i)}
    data = json.dumps(data)
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, data=data, headers=headers)
    print response.content


def put(url, data):
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, data=data, headers=headers)
    print response.content


def get(url, key):
    params = {'key': key}
    headers = {'key': key}
    response = requests.get(url=url, params=params, headers=headers)
    print response.content
    print response.content


def get_data(i):
    json_data = {}
    json_data['key'] = str(i)
    json_data['value'] = {'column' + str(i): 'value' + str(i)}
    json_data = json.dumps(json_data)
    return json_data


def random_ip(num, hosts, ports):
    if num == 1:
        ip = hosts + ":" + ports
    else:
        index = random.randint(0, num - 1)
        ip = 'http://'+hosts[index] + ":" + ports[index]
    return ip


if __name__ == '__main__':
    # get("http://172.17.203.218:8500/process", '5')
    pov_n = 2
    host = ['172.17.203.218', '172.17.214.44']
    port = ['8500', '8500']
    data_n = 10
    test_put(pov_n, host, port, data_n)
    # test_branch_put(pov_n, host, port, data_n)

    # data = json.loads(get_data(1))
    # parser = argparse.ArgumentParser()
    # parser.add_argument('-i', '--item', dest='item', nargs='?', default='')
    # args = parser.parse_args()
    # print get_data(args.item)
