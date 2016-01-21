import time
import pytest
import socket

import knit

@pytest.yield_fixture
def k():
    knitter = knit.Knit(nm_port=8020, rm_port=8088)
    yield knitter

def test_cmd(k):
    cmd = "/opt/anaconda/bin/python -c 'import socket; print(socket.gethostname()*2)'"
    appId = k.start_application(cmd)
    
    status = k.get_application_status(appId)
    while status['app']['finalStatus'] != 'SUCCEEDED':
        status = k.get_application_status(appId)
        time.sleep(2)
    
    hostname = socket.gethostname()*2
    logs = k.get_application_logs(appId, shell=True)
    print(logs)
     
    assert hostname in logs
